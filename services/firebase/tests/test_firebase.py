from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from store import MemoryStore,FirestoreStore,SafetyError,timestamp
from collector import reviewed,seed_catalogue,event_identity,upsert_notice,claim_source,collect_one,run
from notifications import quiet,eligible,plan_user,dispatch,fixed_payload,fcm_message,run as notification_run,reset_cycle,completed_cycle,attendance_context,plan_attendance_user
from admin import validate_change,apply_requests,cleanup_accounts,cleanup_execution_retention
from import_pending_pickups import validate_bundle,apply_bundle

NOW=datetime(2026,10,11,10,tzinfo=timezone.utc)
SOURCE={'id':'source','game_id':'blue-archive','parser':'steam_news','steam_appid':123,'url':'https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid=123','approved_for_staging':True,'access_status':'approved','terms_url':'https://steamcommunity.com/dev/apiterms','terms_checked_at':NOW.isoformat(),'notice_hosts_verified_at':NOW.isoformat(),'allowed_notice_hosts':['steamcommunity.com'],'feed_allowlist':['steam_community_announcements'],'server_region':'UNSPECIFIED','server_label':None,'collection_interval_minutes':15}
def response(title='Official update',date=None,feed='steam_community_announcements',appid=123):
 return json.dumps({'appnews':{'appid':appid,'newsitems':[{'gid':'1','title':title,'contents':'Official publisher notice','url':'https://steamcommunity.com/announcement/1','appid':appid,'feedname':feed,'date':int((date or NOW-timedelta(hours=1)).timestamp())}]}}).encode()
def document(title='Official pickup',region='UNSPECIFIED',sha='abc'):
 return {'external_key':'1','raw_sha256':sha,'raw_json':{'title':title},'raw_publication_timestamp':int(NOW.timestamp()),'published_at':NOW.isoformat(),'source_updated_at':None,'observed_at':NOW.isoformat(),'official_url':'https://steamcommunity.com/announcement/1','original_title':title,'original_description':'Official description','original_language':'unknown','server_region':region,'server_label':None,'event_type_candidates':['pickup'],'pickups':[{'server_region':region,'server_label':None,'pickup_kind':None,'official_url':'https://steamcommunity.com/announcement/1'}]}
def owner_store():
 event={'id':'event','gameIds':['blue-archive'],'serverRegion':'KR','serverLabel':None,'verificationStatus':'verified','status':'upcoming','category':'recruitment','startsAt':NOW,'reservationStartsAt':NOW+timedelta(days=1),'revision':1,'title':'Official pickup','description':'Verified schedule'}
 return MemoryStore({'users/alice':{'language':'ko','regions':['KR'],'timezone':'UTC'},'users/alice/userGames/blue-archive':{'enabled':True},'users/alice/notificationSettings/global':{'enabled':True,'regions':['KR'],'stages':['start','1d'],'eventCategories':['all'],'timezone':'UTC'},'users/alice/notificationSubscriptions/device':{'uid':'alice','transport':'fcm','token':'fake-test-token','platform':'android','enabled':True,'expiresAt':None},'gameEvents/event':event}),event

class FirebaseCollectorTests(unittest.TestCase):
 def test_apply_defaults_fail_closed(self):
  with self.assertRaises(SafetyError):FirestoreStore({})
 def test_missing_admin_credentials_fail_closed_before_sdk(self):
  with self.assertRaisesRegex(SafetyError,'credentials_not_configured'):FirestoreStore({'ORBIT_FIREBASE_APPLY':'true','ORBIT_FIREBASE_SPARK_CONFIRMED':'true','FIREBASE_PROJECT_ID':'orbit-example'})
 def test_missing_source_permission_denied(self):self.assertFalse(reviewed({**SOURCE,'access_status':'unknown'}))
 def test_terms_and_notice_host_review_required(self):self.assertFalse(reviewed({**SOURCE,'notice_hosts_verified_at':None}));self.assertFalse(reviewed({**SOURCE,'terms_url':''}))
 def test_seed_keeps_sources_disabled_and_unknown_resets(self):
  s=MemoryStore();seed_catalogue(s,[{'id':'blue-archive','serviceStatus':'UNKNOWN'}],{'games':[{'gameId':'blue-archive','sources':[SOURCE]}]});self.assertFalse(s.get('gameSources/source')['enabled']);self.assertEqual(s.get('games/blue-archive')['resetRules'],[])
 def test_seed_is_change_only_and_preserves_verified_reset(self):
  s=MemoryStore();games=[{'id':'blue-archive'}];r={'games':[]};seed_catalogue(s,games,r);s.records['games/blue-archive']['resetRules']=[{'id':'official'}];s.records['games/blue-archive']['verifiedResetRuleIds']=['official'];w=s.writes;seed_catalogue(s,games,r);self.assertEqual(s.writes,w)
 def test_reseed_preserves_source_health_timestamps(self):
  s=MemoryStore();g=[{'id':'blue-archive'}];r={'games':[{'gameId':'blue-archive','sources':[SOURCE]}]};seed_catalogue(s,g,r);s.records['gameSources/source'].update(lastSuccessAt=NOW,lastErrorAt=NOW-timedelta(days=1),enabled=True);w=s.writes;seed_catalogue(s,g,r);self.assertEqual(s.writes,w);self.assertEqual(s.get('gameSources/source')['lastSuccessAt'],NOW)
 def test_identity_separates_game_region_server(self):
  d=document();self.assertNotEqual(event_identity(d,SOURCE),event_identity({**d,'server_region':'JP'},SOURCE));self.assertNotEqual(event_identity(d,SOURCE),event_identity(d,{**SOURCE,'game_id':'other'}));self.assertNotEqual(event_identity(d,SOURCE),event_identity({**d,'server_label':'server2'},SOURCE))
 def test_publication_never_becomes_event_schedule(self):
  s=MemoryStore();upsert_notice(s,SOURCE,document(),NOW);e=s.list('gameEvents')[0][1];self.assertIsNone(e['startsAt']);self.assertEqual(e['verificationStatus'],'pending')
 def test_event_and_raw_revision_deduplicate(self):
  s=MemoryStore();d=document();upsert_notice(s,SOURCE,d,NOW);w=s.writes;self.assertEqual(upsert_notice(s,SOURCE,d,NOW+timedelta(minutes=1)),'unchanged');self.assertEqual(w,s.writes)
 def test_changed_notice_preserves_schedule_and_revokes_approval(self):
  s=MemoryStore();d=document();upsert_notice(s,SOURCE,d,NOW);key=s.list('gameEvents')[0][0];s.records['gameEvents/'+key].update(startsAt=NOW,category='recruitment',verificationStatus='verified');upsert_notice(s,SOURCE,{**d,'raw_sha256':'changed','original_title':'Corrected'},NOW);e=s.get('gameEvents/'+key);self.assertEqual(e['startsAt'],NOW);self.assertEqual(e['category'],'recruitment');self.assertEqual(e['verificationStatus'],'pending');self.assertEqual(e['revision'],2)
 def test_confirmed_pickup_targets_survive_raw_reverify(self):
  s=MemoryStore();d=document();upsert_notice(s,SOURCE,d,NOW);key=s.list('pickupSchedules')[0][0];s.records['pickupSchedules/'+key].update(characters=['Confirmed character'],startsAt=NOW,verificationStatus='verified');upsert_notice(s,SOURCE,{**d,'raw_sha256':'changed'},NOW);r=s.get('pickupSchedules/'+key);self.assertEqual(r['characters'],['Confirmed character']);self.assertEqual(r['startsAt'],NOW);self.assertEqual(r['verificationStatus'],'pending')
 def test_active_lease_cannot_be_reclaimed(self):
  s=MemoryStore();self.assertTrue(claim_source(s,'source',NOW,'backfill','a'));self.assertFalse(claim_source(s,'source',NOW,'backfill','b'))
 def test_expired_lease_recovers(self):
  s=MemoryStore();claim_source(s,'source',NOW,'backfill','a');self.assertTrue(claim_source(s,'source',NOW+timedelta(minutes=13),'backfill','b'))
 def test_daily_reverify_is_24_hour_gated(self):
  s=MemoryStore({'collectorLocks/source':{'lastReverifiedAt':NOW}});self.assertFalse(claim_source(s,'source',NOW+timedelta(hours=1),'reverify','a'))
 def test_failure_keeps_existing_event_and_backoff(self):
  s=MemoryStore({'gameEvents/existing':{'title':'Keep'}});r=collect_one(s,SOURCE,NOW-timedelta(hours=72),NOW,fetch=lambda _: (503,b'',{}),sleep=lambda _:None,now=NOW);self.assertEqual(r['status'],'FETCH_FAILED');self.assertEqual(s.get('gameEvents/existing')['title'],'Keep');self.assertEqual(len(s.list('collectionErrors')),1);self.assertGreater(timestamp(s.get('collectorLocks/source')['nextRunAt']),NOW)
 def test_source_429_retry_after_respected(self):
  s=MemoryStore();collect_one(s,SOURCE,NOW-timedelta(hours=72),NOW,fetch=lambda _: (429,b'',{'retry-after':'7200'}),sleep=lambda _:None,now=NOW);self.assertGreaterEqual(timestamp(s.get('collectorLocks/source')['nextRunAt']),NOW+timedelta(hours=2))
 def test_success_records_actual_metrics_without_schedule(self):
  s=MemoryStore();r=collect_one(s,SOURCE,NOW-timedelta(hours=72),NOW,fetch=lambda _: (200,response(),{}),sleep=lambda _:None,now=NOW);self.assertEqual(r['status'],'success');self.assertEqual(r['metrics']['created'],1);self.assertEqual(r['metrics']['verifiedSchedules'],0)
 def test_72hour_window_cap(self):
  with self.assertRaises(SafetyError):collect_one(MemoryStore(),SOURCE,NOW-timedelta(hours=73),NOW,sleep=lambda _:None)
 def test_wrong_provider_game_id_rejected(self):
  s=MemoryStore();r=collect_one(s,SOURCE,NOW-timedelta(hours=72),NOW,fetch=lambda _: (200,response(appid=999),{}),sleep=lambda _:None,now=NOW);self.assertEqual(r['status'],'FETCH_FAILED');self.assertEqual(s.list('gameEvents'),[])
 def test_unreviewed_feed_does_not_create_event(self):
  s=MemoryStore();r=collect_one(s,SOURCE,NOW-timedelta(hours=72),NOW,fetch=lambda _: (200,response(feed='unofficial'),{}),sleep=lambda _:None,now=NOW);self.assertEqual(r['metrics']['created'],0)
 def test_no_credentials_live_staging_has_zero_remote_writes(self):
  r=run({'games':[{'gameId':'blue-archive','sources':[SOURCE]}]},MemoryStore(),NOW-timedelta(hours=72),NOW,fetch=lambda _:(200,response(),{}),sleep=lambda _:None);self.assertEqual(r['remoteFirestoreWrites'],0);self.assertEqual(r['fcmSends'],0)

class FirebaseDailyReverifyRecoveryTests(unittest.TestCase):
 def collect(self,store,now=NOW,mode='incremental',raw=None,fetch=None):
  with patch('collector.datetime') as clock:
   clock.now.return_value=now
   return collect_one(store,SOURCE,now-timedelta(hours=72),now,mode=mode,
    fetch=fetch or (lambda _:(200,raw or response(date=now-timedelta(days=7)),{})),sleep=lambda _:None,now=now)
 def test_first_due_incremental_revalidates_bounded_older_notices(self):
  s=MemoryStore({'gameSources/source':{'enabled':True}});upsert_notice(s,SOURCE,document(),NOW);r=self.collect(s)
  self.assertEqual((r['requestedMode'],r['mode']),('incremental','reverify'));self.assertEqual(r['metrics']['updated'],1);self.assertEqual(r['metrics']['created'],0);self.assertEqual(r['metrics']['pagesFetched'],1);self.assertEqual(timestamp(s.get('collectorLocks/source')['lastReverifiedAt']),NOW);self.assertEqual(timestamp(s.get('gameSources/source')['lastReverifiedAt']),NOW)
 def test_recently_reverified_source_retains_72hour_incremental_scope(self):
  s=MemoryStore({'collectorLocks/source':{'lastReverifiedAt':NOW-timedelta(hours=23)}});r=self.collect(s)
  self.assertEqual(r['mode'],'incremental');self.assertEqual(r['metrics']['created'],0);self.assertEqual(s.list('gameEvents'),[])
 def test_exact_24hour_boundary_selects_reverify_and_records_health(self):
  s=MemoryStore({'collectorLocks/source':{'lastReverifiedAt':NOW-timedelta(hours=24)},'gameSources/source':{'enabled':True}});r=self.collect(s)
  self.assertEqual(r['mode'],'reverify');self.assertEqual(s.get('gameSources/source')['lastReverifiedAt'],NOW)
 def test_overdue_reverify_does_not_bypass_source_interval(self):
  s=MemoryStore({'collectorLocks/source':{'nextRunAt':NOW+timedelta(minutes=1),'lastReverifiedAt':NOW-timedelta(days=2)}});before=s.writes
  r=self.collect(s,fetch=lambda _:self.fail('not-due HTTP'))
  self.assertEqual(r['status'],'DEFERRED');self.assertEqual(s.writes,before)
 def test_overdue_reverify_does_not_reclaim_active_lease(self):
  s=MemoryStore({'collectorLocks/source':{'leaseUntil':NOW+timedelta(minutes=1),'token':'other','lastReverifiedAt':NOW-timedelta(days=2)}})
  self.assertEqual(self.collect(s,fetch=lambda _:self.fail('active-lease HTTP'))['status'],'DEFERRED');self.assertEqual(s.get('collectorLocks/source')['token'],'other')
 def test_daily_deferred_after_incremental_recovers_at_next_due_tick(self):
  s=MemoryStore({'collectorLocks/source':{'nextRunAt':NOW+timedelta(minutes=15),'lastReverifiedAt':NOW-timedelta(days=2)}})
  self.assertEqual(self.collect(s,mode='reverify',fetch=lambda _:self.fail('daily-not-due HTTP'))['status'],'DEFERRED')
  r=self.collect(s,now=NOW+timedelta(minutes=15));self.assertEqual(r['mode'],'reverify');self.assertEqual(r['status'],'success');self.assertEqual(s.get('collectorLocks/source')['lastReverifiedAt'],NOW+timedelta(minutes=15))
 def test_failed_reverify_keeps_last_success_and_retry_backoff(self):
  old=NOW-timedelta(days=2);s=MemoryStore({'collectorLocks/source':{'lastReverifiedAt':old}});calls=[]
  def failure(_):calls.append(True);return 429,b'',{'retry-after':'7200'}
  r=self.collect(s,fetch=failure);self.assertEqual(r['mode'],'reverify');self.assertEqual(r['status'],'FETCH_FAILED');self.assertEqual(s.get('collectorLocks/source')['lastReverifiedAt'],old);self.assertEqual(s.get('collectorLocks/source')['nextRunAt'],NOW+timedelta(hours=2));self.assertEqual(self.collect(s,now=NOW+timedelta(minutes=15),fetch=failure)['status'],'DEFERRED');self.assertEqual(len(calls),1)
 def test_explicit_backfill_is_not_promoted_and_keeps_window(self):
  s=MemoryStore();r=self.collect(s,mode='backfill');self.assertEqual(r['mode'],'backfill');self.assertEqual(r['metrics']['created'],0);self.assertNotIn('lastReverifiedAt',s.get('collectorLocks/source'))
 def test_reverify_does_not_insert_fifty_unseen_older_notices(self):
  payload=json.loads(response(date=NOW-timedelta(days=7)));base=payload['appnews']['newsitems'][0];payload['appnews']['newsitems']=[{**base,'gid':str(i),'url':'https://steamcommunity.com/announcement/'+str(i)} for i in range(50)]
  s=MemoryStore();r=self.collect(s,raw=json.dumps(payload).encode());self.assertEqual(r['metrics']['olderUnseenSkipped'],50);self.assertEqual(r['metrics']['created'],0);self.assertEqual(s.list('gameEvents'),[]);self.assertEqual(s.list('collectionDocuments'),[])
 def test_other_source_existing_id_does_not_authorize_old_insertion(self):
  s=MemoryStore();upsert_notice(s,SOURCE,document(),NOW);key=s.list('gameEvents')[0][0];s.records['gameEvents/'+key]['sourceId']='another-source';r=self.collect(s);self.assertEqual(r['metrics']['knownEventsForReverify'],0);self.assertEqual(r['metrics']['olderUnseenSkipped'],1);self.assertEqual(r['metrics']['updated'],0)
 def test_source_equality_query_is_filtered_and_bounded(self):
  s=MemoryStore({'gameEvents/'+str(i):{'sourceId':'source' if i>=10 else 'other'} for i in range(80)});rows=s.list_equal('gameEvents','sourceId','source',50);self.assertEqual(len(rows),50);self.assertTrue(all(row['sourceId']=='source' for _,row in rows));self.assertEqual(s.reads,50)
  with self.assertRaisesRegex(SafetyError,'bounded_source_query'):s.list_equal('gameEvents','sourceId','source',51)
 def test_source_query_honors_remote_read_budget_before_sdk_query(self):
  s=FirestoreStore.__new__(FirestoreStore);s.reads=2500;s.max_reads=2500;s.writes=0;s.max_writes=500
  with self.assertRaisesRegex(SafetyError,'soft_budget'):s.list_equal('gameEvents','sourceId','source',50)

class FirebaseExplicitPendingSupplementTests(unittest.TestCase):
 def bundle(self):
  old=document();event_id=event_identity(old,SOURCE)
  return {'source':SOURCE,'olderNotice':old,'currentNoticeEventId':'current','recordedAt':NOW,
   'pickups':[{'endYearExplicit':False,'data':{'id':'outfit','eventId':event_id,'gameId':'blue-archive','serverRegion':'UNSPECIFIED','serverLabel':None,'officialUrl':'https://steamcommunity.com/announcement/1','verificationStatus':'pending','pickupKind':'outfit','characters':[],'weapons':[],'outfits':['Official outfit'],'startsAt':None,'endsAt':None}}]}
 def store(self):return MemoryStore({'gameEvents/current':{'sourceId':'source','gameIds':['blue-archive']}})
 def test_supplement_is_create_only_and_idempotent(self):
  s=self.store();b=self.bundle();self.assertEqual(apply_bundle(s,b)['pickupsCreated'],1);w=s.writes;again=apply_bundle(s,b);self.assertEqual(again['noticesCreated'],0);self.assertEqual(again['existingPickupsPreserved'],1);self.assertEqual(s.writes,w);self.assertEqual(s.get('pickupSchedules/outfit')['verificationStatus'],'pending')
 def test_missing_current_official_reference_blocks_before_any_write(self):
  s=MemoryStore()
  with self.assertRaisesRegex(SafetyError,'current_notice_reference_missing'):apply_bundle(s,self.bundle())
  self.assertEqual(s.writes,0)
 def test_existing_manually_verified_pickup_is_preserved(self):
  s=self.store();s.records['pickupSchedules/outfit']={'verificationStatus':'verified','startsAt':NOW,'outfits':['Confirmed outfit']};r=apply_bundle(s,self.bundle());self.assertEqual(r['existingPickupsPreserved'],1);self.assertEqual(s.get('pickupSchedules/outfit')['verificationStatus'],'verified');self.assertEqual(s.get('pickupSchedules/outfit')['outfits'],['Confirmed outfit'])
 def test_unconfirmed_end_year_cannot_become_stored_timestamp(self):
  b=self.bundle();b['pickups'][0]['data']['endsAt']=NOW
  with self.assertRaisesRegex(SafetyError,'explicit_end_year_required'):validate_bundle(b)

class FirebaseNotificationTests(unittest.TestCase):
 def test_task_reminders_never_eligible(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',e,'TASK_REMINDER',NOW))
 def test_unregistered_game_never_notified(self):
  s,e=owner_store();s.delete('users/alice/userGames/blue-archive');self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_unknown_region_is_not_global(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',{**e,'serverRegion':'UNSPECIFIED'},'event:start',NOW))
 def test_global_requires_explicit_interest(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',{**e,'serverRegion':'GLOBAL'},'event:start',NOW))
 def test_pending_event_never_notified(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',{**e,'verificationStatus':'pending'},'event:start',NOW))
 def test_cancelled_event_not_notified(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',{**e,'status':'cancelled'},'event:start',NOW))
 def test_quiet_hours_cross_midnight_and_equal_all_day(self):
  self.assertTrue(quiet({'quietStart':'23:00','quietEnd':'07:00','timezone':'UTC'},NOW.replace(hour=1)));self.assertTrue(quiet({'quietStart':'10:00','quietEnd':'10:00','timezone':'UTC'},NOW))
 def test_event_category_filter(self):
  s,e=owner_store();s.records['users/alice/notificationSettings/global']['eventCategories']=['update'];self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_event_stages_and_offsets_independently_filter(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',e,'event:1h',NOW));s.records['users/alice/notificationSettings/global']['offsets']=[1440];self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_reservation_and_event_separate_dedupe(self):
  s,e=owner_store();self.assertEqual(plan_user(s,'alice',[e],NOW),2);self.assertEqual(plan_user(s,'alice',[e],NOW),0);kinds={j['kind']for _,j in s.list('users/alice/notificationJobs')};self.assertEqual(kinds,{'event:start','reservation:1d'})
 def test_sender_rechecks_unregister_after_plan(self):
  s,e=owner_store();plan_user(s,'alice',[e],NOW);s.delete('users/alice/userGames/blue-archive');key=s.list('users/alice/notificationJobs')[0][0];calls=[];self.assertEqual(dispatch(s,'alice',key,lambda *a:calls.append(a),NOW),'cancelled');self.assertEqual(calls,[])
 def test_provider_acceptance_is_not_human_receipt(self):
  s,e=owner_store();s.put('users/alice/notificationJobs/test',{'uid':'alice','kind':'push_test','status':'requested','language':'ko'});self.assertEqual(dispatch(s,'alice','test',lambda *_:'provider-id',NOW),'provider_accepted');self.assertIsNone(s.get('users/alice/notificationJobs/test')['humanConfirmedAt']);self.assertEqual(dispatch(s,'alice','test',lambda *_:self.fail('duplicate'),NOW),'skipped')
 def test_failed_ambiguous_send_is_not_retried(self):
  s,e=owner_store();s.put('users/alice/notificationJobs/test',{'uid':'alice','kind':'push_test','status':'requested'});calls=[]
  def send(*a):calls.append(a);raise TimeoutError()
  self.assertEqual(dispatch(s,'alice','test',send,NOW),'delivery_unconfirmed');dispatch(s,'alice','test',send,NOW);self.assertEqual(len(calls),1)
 def test_legacy_task_job_cancelled_without_provider(self):
  s,e=owner_store();s.put('users/alice/notificationJobs/old',{'kind':'TASK_REMINDER','status':'requested'});dispatch(s,'alice','old',lambda *_:self.fail('task send'),NOW);self.assertEqual(s.get('users/alice/notificationJobs/old')['status'],'cancelled')
 def test_no_subscription_uses_inapp_inbox(self):
  s,e=owner_store();s.delete('users/alice/notificationSubscriptions/device');s.put('users/alice/notificationJobs/test',{'uid':'alice','kind':'push_test','status':'requested'});self.assertEqual(dispatch(s,'alice','test',lambda *_:self.fail('no token'),NOW),'in_app_only');self.assertIsNotNone(s.get('users/alice/notifications/test'))
 def test_native_windows_fcm_not_falsely_supported(self):
  s,e=owner_store();s.records['users/alice/notificationSubscriptions/device']['platform']='windows';s.put('users/alice/notificationJobs/test',{'uid':'alice','kind':'push_test','status':'requested'});self.assertEqual(dispatch(s,'alice','test',lambda *_:self.fail('unsupported'),NOW),'in_app_only')
 def test_expired_subscription_excluded(self):
  s,e=owner_store();s.records['users/alice/notificationSubscriptions/device']['expiresAt']=NOW;s.put('users/alice/notificationJobs/test',{'uid':'alice','kind':'push_test','status':'requested'});self.assertEqual(dispatch(s,'alice','test',lambda *_:self.fail('expired'),NOW),'in_app_only')
 def test_schedule_revision_cancels_stale_job(self):
  s,e=owner_store();plan_user(s,'alice',[e],NOW);s.records['gameEvents/event']['revision']=2;key=s.list('users/alice/notificationJobs')[0][0];self.assertEqual(dispatch(s,'alice',key,lambda *_:self.fail('old schedule'),NOW),'cancelled')
 def test_deleted_account_cannot_receive(self):
  s,e=owner_store();s.records['users/alice']['deletionRequestedAt']=NOW;self.assertFalse(eligible(s,'alice',None,'push_test',NOW))
 def test_crashed_send_quarantined_after_lease(self):
  s,e=owner_store();s.put('users/alice/notificationJobs/crashed',{'kind':'push_test','status':'sending','updatedAt':NOW-timedelta(minutes=13)});notification_run(s,lambda *_:self.fail('ambiguous retry'),NOW);self.assertEqual(s.get('users/alice/notificationJobs/crashed')['status'],'delivery_unconfirmed')

class FirebaseMessagingContractTests(unittest.TestCase):
 def test_profile_global_switch_rechecked_even_with_enabled_game_preferences(self):
  s,e=owner_store();s.records['users/alice']['notificationsEnabled']=False;self.assertFalse(eligible(s,'alice',e,'event:start',NOW));self.assertTrue(eligible(s,'alice',None,'push_test',NOW))
 def test_profile_quiet_hours_rechecked_when_settings_stale(self):
  s,e=owner_store();s.records['users/alice'].update(dndStart='09:00',dndEnd='11:00');self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_global_type_switch_cannot_be_bypassed_by_game_settings(self):
  s,e=owner_store();s.records['users/alice/notificationSettings/global']['types']=['reservation'];s.put('users/alice/notificationSettings/blue-archive',{'enabled':True,'types':['event'],'regions':['KR']});self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_game_type_switch_enforced(self):
  s,e=owner_store();s.put('users/alice/notificationSettings/blue-archive',{'enabled':True,'types':['reservation'],'regions':['KR']});self.assertFalse(eligible(s,'alice',e,'event:start',NOW));self.assertTrue(eligible(s,'alice',e,'reservation:1d',NOW))
 def test_unknown_job_kind_fails_closed_without_exception(self):
  s,e=owner_store();self.assertFalse(eligible(s,'alice',e,'unsafe',NOW))
 def test_fcm_data_only_receiver_contract(self):
  class Messaging:
   Message=staticmethod(lambda **kw:kw)
   AndroidConfig=staticmethod(lambda **kw:kw)
  p={'title':'Title','body':'Body','url':'/settings','jobId':'job','tag':'dedupe','recipientUid':'alice'};m=fcm_message(Messaging,'fake-token',p)
  self.assertNotIn('notification',m);self.assertNotIn('webpush',m);self.assertEqual(m['data']['recipientUid'],'alice');self.assertEqual(m['data']['dedupeKey'],'dedupe');self.assertEqual(m['data']['gameIds'],'[]');self.assertEqual(m['android']['priority'],'high')

class FirebaseNativePayloadTests(unittest.TestCase):
 def test_event_primary_game_is_eligible_not_first_unregistered_game(self):
  s,e=owner_store();e={**e,'gameIds':['not-registered','blue-archive']};p=fixed_payload(s,'alice',{'id':'job','kind':'event:start'},e,NOW);self.assertEqual(p['gameId'],'blue-archive');self.assertEqual(p['gameIds'],['blue-archive']);self.assertEqual(p['category'],'recruitment');self.assertEqual(p['eventId'],'event')
 def test_explicit_test_has_kind_without_fabricated_game(self):
  s,e=owner_store();p=fixed_payload(s,'alice',{'id':'job','kind':'push_test'});self.assertEqual(p['kind'],'push_test');self.assertEqual(p['gameId'],'');self.assertEqual(p['gameIds'],[])
 def test_large_multibyte_notification_fits_data_limit(self):
  class Messaging:
   Message=staticmethod(lambda **kw:kw)
   AndroidConfig=staticmethod(lambda **kw:kw)
  p={'title':'韓'*500,'body':'日'*5000,'url':'/events/e','jobId':'job','tag':'dedupe','recipientUid':'alice','gameId':'game','gameIds':['game'+str(n)for n in range(1000)]};m=fcm_message(Messaging,'fake-token',p);self.assertLess(len(json.dumps(m['data'],ensure_ascii=False).encode()),3500);self.assertEqual(json.loads(m['data']['gameIds']),['game'])

class FirebaseRecipientIsolationTests(unittest.TestCase):
 def test_all_payloads_use_authoritative_target_uid_not_job_uid(self):
  s,e=owner_store()
  for kind in ['push_test','event:start']:
   p=fixed_payload(s,'alice',{'id':'job','kind':kind,'uid':'mallory','recipientUid':'mallory'},e,NOW)
   self.assertEqual(p['recipientUid'],'alice')
 def test_missing_recipient_uid_fails_closed_before_provider_message(self):
  class Messaging:
   Message=staticmethod(lambda **kw: (_ for _ in ()).throw(AssertionError('provider message built without recipient')))
   AndroidConfig=staticmethod(lambda **kw:kw)
  with self.assertRaisesRegex(SafetyError,'recipient_uid_required'):
   fcm_message(Messaging,'fake-token',{'title':'Title','body':'Body','url':'/settings','jobId':'job','tag':'tag'})

class FirebaseAttendanceNotificationTests(unittest.TestCase):
 def fixture(self):
  store,_=owner_store();rule={'id':'kr-daily','verified':True,'timezone':'UTC','dailyTime':'04:00','evidenceUrl':'https://official.example/reset','region':'KR'}
  store.records['games/blue-archive']={'names':{'ko':'테스트 게임','en':'Test game','ja':'テストゲーム'},'resetRules':[rule],'verifiedResetRuleIds':['kr-daily'],'resetRuleById':{'kr-daily':rule}}
  store.records['users/alice/userGames/blue-archive'].update(resetRuleId='kr-daily',serverRegion='KR')
  store.records['users/alice/notificationSettings/global'].update(attendanceEnabled=True,attendanceDeliveryMode='remote',reminderTimes=['10:00'])
  return store,rule
 def summary(self,cycle):
  return {'gameId':'blue-archive','completionStatus':'completed','source':'device_observed','executionCount':1,'verifiedResetRuleId':cycle['ruleId'],'cycleKey':cycle['cycleKey'],'timezone':cycle['timezone'],'cycleStart':cycle['cycleStart'],'cycleEnd':cycle['cycleEnd'],'firstExecutionAt':NOW,'lastExecutionAt':NOW,'lastExecutionRecordId':'expired-raw-id'}
 def complete(self,store,rule):
  cycle=reset_cycle(rule,NOW);store.put('users/alice/attendanceDaily/blue-archive_'+rule['id']+'_'+cycle['localDate'],self.summary(cycle));return cycle
 def test_reset_boundary_uses_verified_rule_and_server_timezone(self):
  _,rule=self.fixture();self.assertEqual(reset_cycle(rule,NOW.replace(hour=3))['localDate'],'2026-10-10');self.assertEqual(reset_cycle(rule,NOW.replace(hour=4))['localDate'],'2026-10-11')
  self.assertIsNone(reset_cycle({**rule,'verified':False},NOW));self.assertIsNone(reset_cycle({**rule,'dailyTime':'25:00'},NOW));self.assertIsNone(reset_cycle({**rule,'evidenceUrl':'http://official.example'},NOW))
 def test_dst_ambiguous_and_nonexistent_resets_are_not_guessed(self):
  _,rule=self.fixture();self.assertIsNone(reset_cycle({**rule,'timezone':'America/New_York','dailyTime':'01:30'},datetime(2026,11,1,8,tzinfo=timezone.utc)));self.assertIsNone(reset_cycle({**rule,'timezone':'America/New_York','dailyTime':'02:30'},datetime(2026,3,8,10,tzinfo=timezone.utc)))
 def test_remote_attendance_is_explicit_opt_in_and_local_mode_never_remote_sends(self):
  store,_=self.fixture()
  for mode in ['local','off',None]:
   store.records['users/alice/notificationSettings/global']['attendanceDeliveryMode']=mode
   self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
  store.records['users/alice/notificationSettings/global'].pop('attendanceDeliveryMode');self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
 def test_existing_game_preference_missing_mode_stays_local_even_with_remote_global(self):
  store,_=self.fixture();store.put('users/alice/notificationSettings/blue-archive',{'attendanceEnabled':True,'enabled':True})
  self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
  store.records['users/alice/notificationSettings/blue-archive']['attendanceDeliveryMode']='remote';self.assertEqual(plan_attendance_user(store,'alice',NOW),1)
 def test_unknown_or_unselected_reset_does_not_create_attendance_job(self):
  store,_=self.fixture();store.records['users/alice/userGames/blue-archive']['resetRuleId']=None;self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
  store.records['users/alice/userGames/blue-archive']['resetRuleId']='guessed';self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
 def test_rule_region_must_match_selected_server(self):
  store,_=self.fixture();store.records['users/alice/userGames/blue-archive']['serverRegion']='JP';self.assertIsNone(attendance_context(store,'alice','blue-archive',NOW))
 def test_completion_on_another_device_stops_current_cycle_before_plan(self):
  store,rule=self.fixture();self.complete(store,rule);self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
 def test_global_record_deletion_marker_invalidates_old_completed_summary_before_cleanup(self):
  store,rule=self.fixture();self.complete(store,rule);store.records['users/alice']['executionRecordsDeletedAt']=NOW
  self.assertEqual(plan_attendance_user(store,'alice',NOW),1)
 def test_a_new_observed_launch_after_deletion_marker_still_stops_reminders(self):
  store,rule=self.fixture();cycle=self.complete(store,rule);store.records['users/alice']['executionRecordsDeletedAt']=NOW-timedelta(seconds=1)
  self.assertIsNone(attendance_context(store,'alice','blue-archive',NOW));self.assertTrue(completed_cycle(self.summary(cycle),'blue-archive',cycle))
 def test_completed_summary_survives_raw_retention_without_new_queries_or_writes(self):
  store,rule=self.fixture();cycle=self.complete(store,rule);self.assertTrue(completed_cycle(self.summary(cycle),'blue-archive',cycle));self.assertIsNone(store.get('users/alice/executionRecords/expired-raw-id'));writes=store.writes;self.assertEqual(plan_attendance_user(store,'alice',NOW),0);self.assertEqual(store.writes,writes)
 def test_other_server_or_prior_cycle_cannot_suppress_current_attendance(self):
  store,rule=self.fixture();cycle=reset_cycle(rule,NOW)
  for row in [{**self.summary(cycle),'verifiedResetRuleId':'jp-daily'},{**self.summary(cycle),'cycleKey':'kr-daily:2026-10-10'},{**self.summary(cycle),'cycleStart':cycle['cycleStart']-timedelta(days=1)}]:
   self.assertFalse(completed_cycle(row,'blue-archive',cycle))
 def test_dedupe_prevents_repeated_same_slot(self):
  store,_=self.fixture();self.assertEqual(plan_attendance_user(store,'alice',NOW),1);writes=store.writes;self.assertEqual(plan_attendance_user(store,'alice',NOW+timedelta(minutes=10)),0);self.assertEqual(store.writes,writes)
 def test_maximum_three_per_cycle_even_after_reminder_time_changes(self):
  store,_=self.fixture()
  for hour in [10,11,12,13]:
   store.records['users/alice/notificationSettings/global']['reminderTimes']=[str(hour)+':00']
   self.assertEqual(plan_attendance_user(store,'alice',NOW.replace(hour=hour)),int(hour!=13))
  self.assertEqual(len(store.list('users/alice/notificationJobs')),3)
 def test_completion_after_plan_is_rechecked_before_fcm_and_cancels(self):
  store,rule=self.fixture();plan_attendance_user(store,'alice',NOW);job_id=store.list('users/alice/notificationJobs')[0][0];self.complete(store,rule)
  self.assertEqual(dispatch(store,'alice',job_id,lambda *_:self.fail('completed user sent'),NOW),'cancelled')
 def test_unsubscribe_after_plan_cancels_attendance_without_a_push(self):
  store,_=self.fixture();plan_attendance_user(store,'alice',NOW);job_id=store.list('users/alice/notificationJobs')[0][0];store.delete('users/alice/userGames/blue-archive')
  self.assertEqual(dispatch(store,'alice',job_id,lambda *_:self.fail('unfollowed game sent'),NOW),'cancelled')
 def test_attendance_payload_has_cycle_and_does_not_claim_in_game_noncompletion(self):
  store,_=self.fixture();plan_attendance_user(store,'alice',NOW);job_id=store.list('users/alice/notificationJobs')[0][0];calls=[]
  self.assertEqual(dispatch(store,'alice',job_id,lambda token,payload:calls.append(payload) or 'provider-id',NOW),'provider_accepted')
  self.assertEqual(calls[0]['kind'],'attendance');self.assertEqual(calls[0]['url'],'/dashboard');self.assertEqual(calls[0]['cycleKey'],'kr-daily:2026-10-11');self.assertEqual(calls[0]['resetRuleId'],'kr-daily');self.assertIn('실행 기록',calls[0]['body']);self.assertNotIn('미완료',calls[0]['body'])
  self.assertEqual(dispatch(store,'alice',job_id,lambda *_:self.fail('repeat send'),NOW),'skipped')
 def test_global_off_and_game_override_off_block_remote_attendance(self):
  store,_=self.fixture();store.records['users/alice/notificationSettings/global']['attendanceEnabled']=False;self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
  store.records['users/alice/notificationSettings/global']['attendanceEnabled']=True;store.put('users/alice/notificationSettings/blue-archive',{'attendanceEnabled':False});self.assertEqual(plan_attendance_user(store,'alice',NOW),0)
 def test_invalid_quiet_timezone_fails_closed_instead_of_stopping_all_users(self):
  self.assertTrue(quiet({'quietStart':'00:00','quietEnd':'23:59','timezone':'Invalid/Zone'},NOW))

class FirebaseAdminTests(unittest.TestCase):
 def test_official_field_whitelist(self):
  with self.assertRaises(SafetyError):validate_change('games','game',{'secretKey':'not-a-real-key'})
 def test_nested_secrets_rejected(self):
  with self.assertRaises(SafetyError):validate_change('games','game',{'names':{'en':'Game'},'officialWebsite':'https://official.example','sourceEvidence':[{'privateKey':'fake'}]})
 def test_unverified_reset_rule_rejected(self):
  with self.assertRaises(SafetyError):validate_change('games','game',{'names':{'en':'Game'},'officialWebsite':'https://official.example','resetRules':[{'id':'guessed','verified':False,'timezone':'UTC','dailyTime':'04:00','evidenceUrl':'https://official.example'}]})
 def test_verified_reset_derive_ids(self):
  rule={'id':'verified','verified':True,'timezone':'UTC','dailyTime':'04:00','evidenceUrl':'https://official.example/reset'};r=validate_change('games','game',{'names':{'en':'Game'},'officialWebsite':'https://official.example','resetRules':[rule],'resetRuleById':{'forged':rule}});self.assertEqual(r['verifiedResetRuleIds'],['verified']);self.assertEqual(r['resetRuleById'],{'verified':rule})
 def test_reset_ids_are_unique_canonical_and_regions_cannot_conflict(self):
  rule={'id':'verified','verified':True,'timezone':'UTC','dailyTime':'04:00','evidenceUrl':'https://official.example/reset'}
  for rules in [[rule,rule],[{**rule,'id':'BAD_ID'}],[{**rule,'region':'KR','serverRegion':'JP'}]]:
   with self.assertRaises(SafetyError):validate_change('games','game',{'names':{'en':'Game'},'officialWebsite':'https://official.example','resetRules':rules})
 def test_verified_event_requires_region_dates_and_evidence(self):
  with self.assertRaises(SafetyError):validate_change('gameEvents','event',{'officialUrl':'https://official.example/notice','verificationStatus':'verified','serverRegion':'UNSPECIFIED'})
 def test_source_enable_requires_review(self):
  with self.assertRaises(SafetyError):validate_change('gameSources','source',{'url':'https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid=123','parser':'steam_news','steamAppid':123,'intervalMinutes':15,'enabled':True})
 def test_pending_outfits_keep_unknown_start_and_region(self):
  row=validate_change('pickupSchedules','outfit',{'officialUrl':'https://official.example/notice','pickupKind':'outfit','outfits':['Official outfit'],'characters':[],'weapons':[],'startsAt':None,'endsAt':None,'verificationStatus':'pending','serverRegion':'UNSPECIFIED'});self.assertEqual(row['outfits'],['Official outfit']);self.assertEqual(row['characters'],[]);self.assertIsNone(row['startsAt'])
 def test_outfit_cannot_be_verified_without_start_and_region(self):
  with self.assertRaisesRegex(SafetyError,'verified_schedule_and_region'):validate_change('pickupSchedules','outfit',{'officialUrl':'https://official.example/notice','pickupKind':'outfit','outfits':['Official outfit'],'startsAt':None,'endsAt':NOW,'verificationStatus':'verified','serverRegion':'UNSPECIFIED','lastVerifiedAt':NOW})
 def test_outfit_targets_need_outfit_kind(self):
  with self.assertRaisesRegex(SafetyError,'outfit_targets_require_outfit_kind'):validate_change('pickupSchedules','outfit',{'officialUrl':'https://official.example/notice','pickupKind':'character','outfits':['Official outfit']})
 def test_outfit_target_names_reject_non_strings(self):
  with self.assertRaisesRegex(SafetyError,'invalid_pickup_target_names'):validate_change('pickupSchedules','outfit',{'officialUrl':'https://official.example/notice','pickupKind':'outfit','outfits':[{'not':'a name'}]})
 def test_complete_official_outfit_schedule_can_be_verified(self):
  row=validate_change('pickupSchedules','outfit',{'officialUrl':'https://official.example/notice','pickupKind':'outfit','outfits':['Official outfit'],'characters':[],'weapons':[],'startsAt':NOW,'endsAt':NOW+timedelta(days=1),'verificationStatus':'verified','serverRegion':'KR','lastVerifiedAt':NOW});self.assertEqual(row['verificationStatus'],'verified');self.assertEqual(row['characters'],[])
 def test_admin_queue_applies_once(self):
  s=MemoryStore({'adminChangeRequests/request':{'uid':'admin','collection':'games','documentId':'game','operation':'upsert','status':'requested','data':{'names':{'en':'Game'},'officialWebsite':'https://official.example'}}});self.assertEqual(apply_requests(s)['applied'],1);self.assertEqual(apply_requests(s)['applied'],0);self.assertEqual(s.get('games/game')['lastAdminRequestId'],'request')
 def test_invalid_admin_request_rejected_without_public_write(self):
  s=MemoryStore({'adminChangeRequests/request':{'uid':'admin','collection':'games','documentId':'game','operation':'upsert','status':'requested','data':{'admin':True}}});self.assertEqual(apply_requests(s)['rejected'],1);self.assertIsNone(s.get('games/game'))
 def test_account_cleanup_removes_all_owned_collections_and_auth(self):
  s,e=owner_store();s.records['users/alice']['deletionRequestedAt']=NOW;s.put('users/alice/pushTestLimits/current',{'uid':'alice'});s.put('users/alice/notifications/n',{'uid':'alice'});deleted=[];r=cleanup_accounts(s,delete_auth=lambda uid:deleted.append(uid));self.assertEqual(r['accountsDeleted'],1);self.assertEqual(deleted,['alice']);self.assertIsNone(s.get('users/alice'));self.assertEqual(s.list('users/alice/notificationSubscriptions'),[])
 def test_account_cleanup_document_limit_keeps_tombstone(self):
  s,e=owner_store();s.records['users/alice']['deletionRequestedAt']=NOW;r=cleanup_accounts(s,max_documents=1);self.assertEqual(r['pending'],1);self.assertIsNotNone(s.get('users/alice'))

class FirebaseExecutionRetentionTests(unittest.TestCase):
 def test_requested_global_deletion_removes_old_raw_and_daily_without_deleting_new_launches_or_receipts(self):
  records={'users/alice':{'uid':'alice','executionRecordsDeletedAt':NOW},'users/alice/executionRecords/old':{'uid':'alice','startedAt':NOW},'users/alice/executionRecords/new':{'uid':'alice','startedAt':NOW+timedelta(seconds=1)},'users/alice/attendanceDaily/old':{'uid':'alice','lastExecutionAt':NOW},'users/alice/attendanceDaily/new':{'uid':'alice','lastExecutionAt':NOW+timedelta(seconds=1)},'users/alice/notificationReceipts/current':{'uid':'alice','createdAt':NOW}}
  store=MemoryStore(records);result=cleanup_execution_retention(store,NOW+timedelta(minutes=1));self.assertEqual(result['explicitMarkerDeletes'],2);self.assertEqual(result['attendanceDailyWrites'],1);self.assertIsNotNone(store.get('users/alice/executionRecords/new'));self.assertIsNotNone(store.get('users/alice/attendanceDaily/new'));self.assertIsNotNone(store.get('users/alice/notificationReceipts/current'))
 def test_requested_global_deletion_also_clears_unconfirmed_daily_rows_older_than_marker(self):
  store=MemoryStore({'users/alice':{'uid':'alice','executionRecordsDeletedAt':NOW},'users/alice/attendanceDaily/unconfirmed':{'uid':'alice','lastExecutionAt':None,'updatedAt':NOW-timedelta(seconds=1),'completionStatus':'unconfirmed'}});result=cleanup_execution_retention(store,NOW+timedelta(minutes=1));self.assertEqual(result['explicitMarkerDeletes'],1);self.assertIsNone(store.get('users/alice/attendanceDaily/unconfirmed'))
 def test_old_launches_and_receipts_are_removed_without_changing_retained_daily_summary(self):
  records={'users/alice':{'uid':'alice'},'users/alice/attendanceDaily/retained':{'uid':'alice','completionStatus':'completed','executionCount':3},'users/alice/executionRecords/old':{'uid':'alice','startedAt':NOW-timedelta(days=31)},'users/alice/executionRecords/new':{'uid':'alice','startedAt':NOW-timedelta(days=1)},'users/alice/notificationReceipts/old':{'uid':'alice','createdAt':NOW-timedelta(days=31)},'users/alice/attendanceReminderLimits/old':{'uid':'alice','createdAt':NOW-timedelta(days=36)}}
  store=MemoryStore(records);result=cleanup_execution_retention(store,NOW)
  self.assertEqual(result['documentsDeleted'],3);self.assertEqual(result['attendanceDailyWrites'],0);self.assertEqual(store.get('users/alice/attendanceDaily/retained'),records['users/alice/attendanceDaily/retained']);self.assertIsNotNone(store.get('users/alice/executionRecords/new'))
 def test_24_hour_gate_and_active_lease_do_not_repeat_remote_writes(self):
  store=MemoryStore({'users/alice':{'uid':'alice'}});cleanup_execution_retention(store,NOW);writes=store.writes;self.assertEqual(cleanup_execution_retention(store,NOW+timedelta(minutes=15))['status'],'DEFERRED');self.assertEqual(store.writes,writes)
  store=MemoryStore({'collectorLocks/execution-retention':{'leaseUntil':NOW+timedelta(minutes=5)}});self.assertEqual(cleanup_execution_retention(store,NOW)['status'],'DEFERRED');self.assertEqual(store.writes,0)
 def test_user_cursor_reaches_users_after_first_twenty(self):
  records={}
  for index in range(21):
   uid=f'user-{index:02d}';records['users/'+uid]={'uid':uid};records[f'users/{uid}/executionRecords/old']={'uid':uid,'startedAt':NOW-timedelta(days=31)}
  store=MemoryStore(records);first=cleanup_execution_retention(store,NOW);self.assertEqual(first['usersVisited'],20);self.assertEqual(first['documentsDeleted'],20);self.assertTrue(first['scanLimitReached']);second=cleanup_execution_retention(store,NOW+timedelta(days=1));self.assertEqual(second['usersVisited'],1);self.assertEqual(second['documentsDeleted'],1);self.assertTrue(second['allUsersScanned'])
 def test_document_cursor_resumes_after_recent_front_page_instead_of_starving_old_records(self):
  records={'users/alice':{'uid':'alice'}}
  for index in range(101):records[f'users/alice/executionRecords/{index:03d}']={'uid':'alice','startedAt':NOW-timedelta(days=1)}
  records['users/alice/executionRecords/999']={'uid':'alice','startedAt':NOW-timedelta(days=31)}
  store=MemoryStore(records);first=cleanup_execution_retention(store,NOW);self.assertEqual(first['recordsScanned'],100);self.assertEqual(first['documentsDeleted'],0);self.assertTrue(first['scanLimitReached']);second=cleanup_execution_retention(store,NOW+timedelta(days=1));self.assertEqual(second['documentsDeleted'],1);self.assertIsNone(store.get('users/alice/executionRecords/999'));self.assertEqual(len(store.list('users/alice/executionRecords')),101)
 def test_exact_boundary_is_retained_and_unknown_dates_are_not_guessed(self):
  store=MemoryStore({'users/alice':{'uid':'alice'},'users/alice/executionRecords/boundary':{'uid':'alice','startedAt':NOW-timedelta(days=30)},'users/alice/executionRecords/invalid':{'uid':'alice','startedAt':'not-a-date'}});result=cleanup_execution_retention(store,NOW);self.assertEqual(result['documentsDeleted'],0);self.assertEqual(result['invalidTimestamps'],1)
 def test_invalid_bounds_are_rejected_before_reading_or_writing(self):
  store=MemoryStore()
  for args in [{'max_users':21},{'max_documents':101},{'max_documents':0}]:
   with self.assertRaises(SafetyError):cleanup_execution_retention(store,NOW,**args)
  self.assertEqual(store.reads,0);self.assertEqual(store.writes,0)
 def test_cursor_query_checks_read_budget_before_sdk_call(self):
  store=FirestoreStore.__new__(FirestoreStore);store.reads=2500;store.max_reads=2500;store.writes=0;store.max_writes=500
  with self.assertRaisesRegex(SafetyError,'soft_budget'):store.list_cursor('users',None,20)
 def test_account_cleanup_includes_receipts_and_server_only_reminder_ledgers(self):
  store=MemoryStore({'users/alice':{'deletionRequestedAt':NOW},'users/alice/notificationReceipts/r':{'uid':'alice'},'users/alice/attendanceReminderLimits/r':{'uid':'alice'}});self.assertEqual(cleanup_accounts(store)['accountsDeleted'],1);self.assertEqual(store.list('users/alice/notificationReceipts'),[]);self.assertEqual(store.list('users/alice/attendanceReminderLimits'),[])

if __name__=='__main__':unittest.main()
