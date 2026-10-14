from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from store import MemoryStore,FirestoreStore,SafetyError,timestamp
from collector import reviewed,seed_catalogue,event_identity,upsert_notice,claim_source,collect_one,run,enrich_document,extract_schedule,highest_rarity_targets,source_row,operating_source,section_documents,confirmed_window_overlap,youtube_request,parse_youtube_documents,YOUTUBE_FEED_DOCUMENTATION,source_policy,EXTRACTION_VERSION,parse_official_roster,refresh_localization_roster,GF2_ROSTER_URLS,new_doll_block_targets,plain_notice
from engine import ValidationError
from notifications import quiet,eligible,plan_user,dispatch,fixed_payload,fcm_message,run as notification_run,reset_cycle,completed_cycle,attendance_context,plan_attendance_user,plan_announcement_user,announcement_identity,ANNOUNCEMENT_KIND
from admin import validate_change,apply_requests,cleanup_accounts,cleanup_execution_retention
from import_pending_pickups import validate_bundle,apply_bundle

NOW=datetime(2026,10,11,10,tzinfo=timezone.utc)
UTC=timezone.utc
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

class FirebaseOfficialSocialFeedTests(unittest.TestCase):
 CHANNEL='UC'+'a'*22
 VIDEO='aBcD123_-xy'
 def source(self,**changes):
  return {**SOURCE,'parser':'youtube_atom','steam_appid':None,'youtube_channel_id':self.CHANNEL,
   'url':'https://www.youtube.com/feeds/videos.xml?channel_id='+self.CHANNEL,'official_host':'www.youtube.com',
   'terms_url':YOUTUBE_FEED_DOCUMENTATION,'allowed_notice_hosts':['www.youtube.com'],'feed_allowlist':['youtube_channel_videos'],
   'publisher_identity_evidence_url':'https://official.example/social','publisher_identity_checked_at':NOW.isoformat(),**changes}
 def feed(self,title='Official livestream',body='Announcement',published=None,updated=None,channel=None,link=None):
  return (f'<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015" xmlns:media="http://search.yahoo.com/mrss/"><id>yt:channel:{self.CHANNEL}</id><entry><id>yt:video:{self.VIDEO}</id><yt:videoId>{self.VIDEO}</yt:videoId><yt:channelId>{channel or self.CHANNEL}</yt:channelId><title>{title}</title><published>{(published or NOW-timedelta(hours=1)).isoformat()}</published><updated>{(updated or NOW-timedelta(minutes=30)).isoformat()}</updated><link rel="alternate" href="{link or "https://www.youtube.com/watch?v="+self.VIDEO}"/><media:group><media:description>{body}</media:description><media:thumbnail url="https://i.ytimg.com/vi/{self.VIDEO}/hqdefault.jpg"/></media:group></entry></feed>').encode()
 def test_review_requires_official_publisher_channel_evidence(self):
  self.assertTrue(reviewed(self.source()));self.assertFalse(reviewed(self.source(publisher_identity_evidence_url=None)));self.assertFalse(reviewed(self.source(publisher_identity_checked_at=None)))
 def test_unsupported_x_source_never_becomes_reviewed(self):
  self.assertFalse(reviewed({**self.source(),'parser':'x_api','url':'https://api.x.com/2/users/example/tweets'}))
 def test_review_requires_exact_feed_documentation_and_allowlist(self):
  self.assertFalse(reviewed(self.source(terms_url='https://www.youtube.com/t/terms')));self.assertFalse(reviewed(self.source(feed_allowlist=['any'])));self.assertFalse(reviewed(self.source(allowed_notice_hosts=['youtube.com','www.youtube.com'])))
 def test_channel_feed_request_never_calls_metered_api(self):
  row=youtube_request(self.source());self.assertEqual(row['official_host'],'www.youtube.com');self.assertEqual(row['method'],'official_feed');self.assertNotIn('googleapis.com',row['url'])
 def test_documented_xml_feed_path_is_supported(self):
  self.assertTrue(youtube_request(self.source(url='https://www.youtube.com/xml/feeds/videos.xml?channel_id='+self.CHANNEL)))
 def test_feed_request_rejects_other_paths_parameters_or_channels(self):
  for url in ['https://www.youtube.com/@example','https://www.youtube.com/feeds/videos.xml?channel_id='+self.CHANNEL+'&key=secret','https://www.youtube.com/feeds/videos.xml?channel_id='+self.CHANNEL+'&other=','https://www.youtube.com/feeds/videos.xml?channel_id='+self.CHANNEL+'#fragment','https://www.youtube.com/feeds/videos.xml?channel_id=UC'+('b'*22),'https://evil.example/feeds/videos.xml?channel_id='+self.CHANNEL]:
   with self.assertRaises(ValidationError):youtube_request(self.source(url=url))
 def test_non_channel_handle_is_not_a_verified_channel_id(self):
  with self.assertRaises(ValidationError):youtube_request(self.source(youtube_channel_id='@OfficialGame'))
 def test_feed_parser_keeps_raw_publication_update_and_video_url(self):
  docs,metrics=parse_youtube_documents(self.feed(),self.source(),NOW.isoformat());self.assertEqual(len(docs),1);self.assertEqual(docs[0]['official_url'],'https://www.youtube.com/watch?v='+self.VIDEO);self.assertTrue(docs[0]['source_updated_at']);self.assertEqual(docs[0]['original_description'],'Announcement');self.assertFalse(metrics['window_coverage_verified'])
 def test_video_publication_is_never_an_event_schedule(self):
  docs,_=parse_youtube_documents(self.feed(),self.source(),NOW.isoformat());row=enrich_document(docs[0],self.source());self.assertIsNone(row['schedule']['startsAt']);self.assertFalse(row['schedule_verified']);self.assertEqual(row['category_candidate'],'official_broadcast')
 def test_thumbnails_remain_unapproved_source_evidence(self):
  docs,_=parse_youtube_documents(self.feed(),self.source(),NOW.isoformat());row=enrich_document(docs[0],self.source());self.assertEqual(row['image_candidates'][0]['rightsStatus'],'UNCONFIRMED');self.assertIsNone(row['approved_image'])
 def test_entry_from_another_channel_rejected(self):
  with self.assertRaisesRegex(ValidationError,'entry_identity'):parse_youtube_documents(self.feed(channel='UC'+'b'*22),self.source(),NOW.isoformat())
 def test_root_from_another_channel_rejected(self):
  raw=self.feed().replace(('yt:channel:'+self.CHANNEL).encode(),('yt:channel:UC'+'b'*22).encode())
  with self.assertRaisesRegex(ValidationError,'response_channel'):parse_youtube_documents(raw,self.source(),NOW.isoformat())
 def test_actual_uc_stripped_headers_accept_only_expected_channel(self):
  raw=self.feed().replace(('yt:channel:'+self.CHANNEL).encode(),('yt:channel:'+self.CHANNEL[2:]).encode()).replace(b'<entry>',('<yt:channelId>'+self.CHANNEL[2:]+'</yt:channelId><entry>').encode())
  docs,_=parse_youtube_documents(raw,self.source(),NOW.isoformat());self.assertEqual(docs[0]['raw_json']['channelId'],self.CHANNEL)
 def test_full_header_and_full_root_channel_id_are_supported(self):
  raw=self.feed().replace(b'<entry>',('<yt:channelId>'+self.CHANNEL+'</yt:channelId><entry>').encode())
  self.assertEqual(len(parse_youtube_documents(raw,self.source(),NOW.isoformat())[0]),1)
 def test_foreign_uc_stripped_root_header_and_channel_id_are_rejected(self):
  for raw in [self.feed().replace(('yt:channel:'+self.CHANNEL).encode(),('yt:channel:'+('b'*22)).encode()),self.feed().replace(b'<entry>',('<yt:channelId>'+('b'*22)+'</yt:channelId><entry>').encode())]:
   with self.assertRaisesRegex(ValidationError,'response_channel'):parse_youtube_documents(raw,self.source(),NOW.isoformat())
 def test_uc_stripped_entry_channel_id_is_never_accepted(self):
  with self.assertRaisesRegex(ValidationError,'entry_identity'):parse_youtube_documents(self.feed(channel=self.CHANNEL[2:]),self.source(),NOW.isoformat())
 def test_shorts_url_has_stable_watch_identity_and_retains_original_evidence(self):
  link='https://www.youtube.com/shorts/'+self.VIDEO;docs,_=parse_youtube_documents(self.feed(link=link),self.source(),NOW.isoformat())
  self.assertEqual(docs[0]['official_url'],'https://www.youtube.com/watch?v='+self.VIDEO);self.assertEqual(docs[0]['raw_json']['originalVideoUrl'],link)
 def test_shorts_cannot_smuggle_other_ids_paths_queries_hosts_or_fragments(self):
  for link in ['https://www.youtube.com/shorts/wrongvideo1','https://www.youtube.com/shorts/'+self.VIDEO+'/extra','https://www.youtube.com/shorts/'+self.VIDEO+'?v=wrongvideo1','https://www.youtube.com/shorts/'+self.VIDEO+'?extra=','https://www.youtube.com/shorts/'+self.VIDEO+'#extra','https://evil.example/shorts/'+self.VIDEO,'http://www.youtube.com/shorts/'+self.VIDEO]:
   with self.assertRaisesRegex(ValidationError,'video_url_mismatch'):parse_youtube_documents(self.feed(link=link),self.source(),NOW.isoformat())
 def test_actual_numbered_thumbnail_hosts_are_evidence_only(self):
  raw=self.feed().replace(b'i.ytimg.com',b'i3.ytimg.com');docs,_=parse_youtube_documents(raw,self.source(),NOW.isoformat());row=enrich_document(docs[0],self.source())
  self.assertTrue(row['image_candidates'][0]['url'].startswith('https://i3.ytimg.com/'));self.assertEqual(row['image_candidates'][0]['rightsStatus'],'UNCONFIRMED');self.assertIsNone(row['approved_image'])
 def test_video_link_from_external_site_or_wrong_video_rejected(self):
  for url in ['https://evil.example/watch?v='+self.VIDEO,'https://www.youtube.com/watch?v=wrongvideo1','https://www.youtube.com/watch?v='+self.VIDEO+'#oops']:
   with self.assertRaises(ValidationError):parse_youtube_documents(self.feed(link=url),self.source(),NOW.isoformat())
 def test_documented_http_video_link_is_canonical_https(self):
  docs,_=parse_youtube_documents(self.feed(link='http://www.youtube.com/watch?v='+self.VIDEO),self.source(),NOW.isoformat());self.assertTrue(docs[0]['official_url'].startswith('https://'))
 def test_xml_dtd_and_entities_rejected(self):
  with self.assertRaisesRegex(ValidationError,'xml_unsafe'):parse_youtube_documents(b'<!DOCTYPE feed [<!ENTITY x "test">]>'+self.feed(),self.source(),NOW.isoformat())
  with self.assertRaisesRegex(ValidationError,'xml_unsafe'):parse_youtube_documents(self.feed().decode().encode('utf-16'),self.source(),NOW.isoformat())
 def test_naive_or_reversed_timestamps_rejected(self):
  with self.assertRaises(ValidationError):parse_youtube_documents(self.feed().replace(b'+00:00',b''),self.source(),NOW.isoformat())
  with self.assertRaises(ValidationError):parse_youtube_documents(self.feed(updated=NOW-timedelta(days=1)),self.source(),NOW.isoformat())
 def test_seed_and_operating_source_preserve_disabled_reviewed_youtube(self):
  store=MemoryStore();source=self.source();seed_catalogue(store,[],{'games':[{'gameId':'blue-archive','sources':[source]}]});row=store.get('gameSources/source');self.assertFalse(row['enabled']);self.assertEqual(row['youtubeChannelId'],self.CHANNEL);self.assertTrue(reviewed(operating_source(row)))
 def test_admin_can_enable_only_reviewed_channel_feed(self):
  row=source_row(self.source(),'blue-archive');row.update(enabled=True);self.assertTrue(validate_change('gameSources','source',row)['enabled']);row['publisherIdentityEvidenceUrl']=None
  with self.assertRaisesRegex(SafetyError,'permission_evidence'):validate_change('gameSources','source',row)
 def test_collector_uses_one_bounded_feed_and_idempotent_storage(self):
  store=MemoryStore();requests=[]
  def fetch(source):requests.append(source['url']);return 200,self.feed(),{}
  result=collect_one(store,self.source(),NOW-timedelta(hours=72),NOW,fetch=fetch,sleep=lambda _:None,now=NOW);self.assertEqual(result['status'],'success');self.assertEqual(result['metrics']['pagesFetched'],1);self.assertFalse(result['metrics']['windowCoverageVerified']);self.assertEqual(len(store.list('gameEvents')),1);self.assertEqual(len(requests),1)
  writes=store.writes;doc,_=parse_youtube_documents(self.feed(),self.source(),NOW.isoformat());self.assertEqual(upsert_notice(store,self.source(),doc[0],NOW),'unchanged');self.assertEqual(store.writes,writes)
 def test_recent_update_of_older_video_is_included_as_source_update(self):
  raw=self.feed(published=NOW-timedelta(days=30),updated=NOW-timedelta(minutes=30));store=MemoryStore();result=collect_one(store,self.source(),NOW-timedelta(hours=72),NOW,fetch=lambda _:(200,raw,{}),sleep=lambda _:None,now=NOW);self.assertEqual(result['metrics']['created'],1);self.assertEqual(timestamp(store.list('gameEvents')[0][1]['publishedAt']),NOW-timedelta(days=30))
 def test_unchanged_old_video_outside_window_is_not_inserted(self):
  raw=self.feed(published=NOW-timedelta(days=30),updated=NOW-timedelta(days=29));store=MemoryStore();result=collect_one(store,self.source(),NOW-timedelta(hours=72),NOW,fetch=lambda _:(200,raw,{}),sleep=lambda _:None,now=NOW);self.assertEqual(result['metrics']['created'],0)
 def test_steam_policy_hash_does_not_change_for_empty_youtube_fields(self):
  self.assertEqual(source_policy(SOURCE),source_policy({**SOURCE,'youtube_channel_id':None,'publisher_identity_evidence_url':None,'publisher_identity_checked_at':None}))
 def test_failed_youtube_feed_preserves_existing_events(self):
  store=MemoryStore({'gameEvents/existing':{'title':'Keep'}});result=collect_one(store,self.source(),NOW-timedelta(hours=72),NOW,fetch=lambda _:(403,b'',{}),sleep=lambda _:None,now=NOW);self.assertEqual(result['status'],'FETCH_FAILED');self.assertEqual(store.get('gameEvents/existing')['title'],'Keep')

class FirebaseNoticeEnrichmentTests(unittest.TestCase):
 def source(self,**changes):
  return {**SOURCE,'server_region':'KR','schedule_extraction_approved':True,'schedule_extraction_reviewed_at':NOW.isoformat(),
   'schedule_extraction_evidence_url':'https://steamcommunity.com/announcement/1',
   'highest_character_rarity':{'labels':['5-star','5성'],'evidenceUrl':'https://official.example/rarity','checkedAt':NOW.isoformat()},**changes}
 def notice(self,body,**changes):
  return {**document(region='KR'),'original_description':body,'raw_json':{'title':'Official pickup','contents':body},'raw_sha256':'fixture-'+str(len(body)),**changes}
 def period(self):return 'Event Duration: 2026-10-13 10:00 - 2026-10-28 18:59 (UTC+8)'
 def inspected_reference_store(self,notice=None):
  source=self.source();notice=notice or self.notice(self.period());store=MemoryStore();upsert_notice(store,source,notice,NOW);key=event_identity(notice,source);row=store.records['gameEvents/'+key]
  url='https://cdn.example/exact-publisher-banner.webp';origin='https://official.example/news';checked=NOW.isoformat()
  row['extractionVersion']=4;row['image']={'url':url,'sourceUrl':notice['official_url'],'kind':'PICKUP_ART','rightsStatus':'UNCONFIRMED',
   'sourceReference':{'mode':'OFFICIAL_NOTICE_IMAGE','sourceUrl':origin,'checkedAt':checked,'allowedImageUrl':url},
   'subjectEvidence':{'status':'MATCHED','eventId':key,'officialUrl':notice['official_url'],'kind':'pickup','method':'reviewed_image','evidenceText':'Exact publisher banner inspected','characterNames':[]}}
  row['sourceImageOverride']={'rawSha256':notice['raw_sha256'],'exactTitle':notice['original_title'],'officialUrl':notice['official_url'],'sourceUrl':origin,'allowedImageUrl':url,'checkedAt':checked}
  return source,notice,store,key
 def test_manual_original_source_reference_survives_extraction_reverify_without_rights_promotion(self):
  source,notice,store,key=self.inspected_reference_store();upsert_notice(store,source,notice,NOW);row=store.get('gameEvents/'+key)
  self.assertEqual(row['image']['rightsStatus'],'UNCONFIRMED');self.assertEqual(row['image']['sourceReference']['mode'],'OFFICIAL_NOTICE_IMAGE');self.assertIsNotNone(row['sourceImageOverride'])
  self.assertEqual(store.list('pickupSchedules')[0][1]['image'],row['image'])
 def test_source_reference_removed_if_raw_notice_changes(self):
  source,notice,store,key=self.inspected_reference_store();upsert_notice(store,source,{**notice,'raw_sha256':'revised-source'},NOW);row=store.get('gameEvents/'+key);self.assertIsNone(row['image']);self.assertIsNone(row['sourceImageOverride'])
 def test_source_reference_removed_for_different_title_url_or_event(self):
  for field,value in [('exactTitle','Wrong poster title'),('allowedImageUrl','https://cdn.example/different.webp'),('officialUrl','https://steamcommunity.com/announcement/another')]:
   source,notice,store,key=self.inspected_reference_store();store.records['gameEvents/'+key]['sourceImageOverride'][field]=value;upsert_notice(store,source,notice,NOW);self.assertIsNone(store.get('gameEvents/'+key)['image'])
  source,notice,store,key=self.inspected_reference_store();store.records['gameEvents/'+key]['image']['subjectEvidence']['eventId']='another-event';upsert_notice(store,source,notice,NOW);self.assertIsNone(store.get('gameEvents/'+key)['image'])
 def test_source_reference_cannot_retain_rejected_image_or_missing_review(self):
  for field,value in [('rightsStatus','REJECTED'),('sourceReference',None)]:
   source,notice,store,key=self.inspected_reference_store();store.records['gameEvents/'+key]['image'][field]=value;upsert_notice(store,source,notice,NOW);self.assertIsNone(store.get('gameEvents/'+key)['image'])
 def test_source_reference_cannot_retain_wrong_subject_character(self):
  source,notice,store,key=self.inspected_reference_store();store.records['gameEvents/'+key]['image']['subjectEvidence']['characterNames']=['Different'];upsert_notice(store,source,notice,NOW);self.assertIsNone(store.get('gameEvents/'+key)['image'])
 def test_source_reference_malformed_review_timestamp_fails_closed(self):
  source,notice,store,key=self.inspected_reference_store();store.records['gameEvents/'+key]['image']['sourceReference']['checkedAt']='yesterday';upsert_notice(store,source,notice,NOW);self.assertIsNone(store.get('gameEvents/'+key)['image'])
 def editorial_language_store(self,language='en',source_language=None):
  source=self.source(content_language=source_language);notice=self.notice(self.period(),original_language='unknown');store=MemoryStore();upsert_notice(store,source,notice,NOW);key=event_identity(notice,source);row=store.records['gameEvents/'+key]
  row.update(extractionVersion=4,originalLanguage=language,languageEvidence={'status':'verified','language':language,'method':'editorial','sourceSha256':notice['raw_sha256'],'checkedAt':NOW.isoformat(),'evidenceUrl':notice['official_url']})
  return source,notice,store,key
 def test_exact_editorial_article_language_survives_extraction_reverify(self):
  for language in ('ko','en','ja'):
   source,notice,store,key=self.editorial_language_store(language);upsert_notice(store,source,notice,NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['originalLanguage'],language);self.assertEqual(row['languageEvidence']['language'],language)
 def test_article_language_review_is_more_specific_than_feed_default(self):
  source,notice,store,key=self.editorial_language_store('ja','en');upsert_notice(store,source,notice,NOW);self.assertEqual(store.get('gameEvents/'+key)['originalLanguage'],'ja')
 def test_changed_raw_notice_revokes_editorial_article_language(self):
  source,notice,store,key=self.editorial_language_store();upsert_notice(store,source,{**notice,'raw_sha256':'source-text-changed'},NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['originalLanguage'],'unknown');self.assertIsNone(row['languageEvidence'])
 def test_changed_raw_notice_uses_current_confirmed_feed_language(self):
  source,notice,store,key=self.editorial_language_store('ja','en');upsert_notice(store,source,{**notice,'raw_sha256':'source-text-changed'},NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['originalLanguage'],'en');self.assertIsNone(row['languageEvidence'])
 def test_arbitrary_old_original_language_without_evidence_not_preserved(self):
  source,notice,store,key=self.editorial_language_store();store.records['gameEvents/'+key].pop('languageEvidence');upsert_notice(store,source,notice,NOW);self.assertEqual(store.get('gameEvents/'+key)['originalLanguage'],'unknown')
 def test_language_review_requires_exact_sha_official_url_and_editorial_method(self):
  for field,value in [('status','pending'),('method','feed_guess'),('sourceSha256','another-sha'),('evidenceUrl','https://steamcommunity.com/announcement/another'),('language','fr'),('checkedAt','yesterday')]:
   source,notice,store,key=self.editorial_language_store();store.records['gameEvents/'+key]['languageEvidence'][field]=value;upsert_notice(store,source,notice,NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['originalLanguage'],'unknown');self.assertIsNone(row['languageEvidence'])
 def test_language_review_and_stored_language_must_agree(self):
  source,notice,store,key=self.editorial_language_store();store.records['gameEvents/'+key]['originalLanguage']='ja';upsert_notice(store,source,notice,NOW);self.assertEqual(store.get('gameEvents/'+key)['originalLanguage'],'unknown')
 def test_valid_article_language_review_propagates_to_derived_character_only_for_same_raw_notice(self):
  source=self.source(content_language=None);notice=self.notice(self.period()+'\nNew 5-star character "First" pickup',original_language='unknown');store=MemoryStore();upsert_notice(store,source,notice,NOW);key=event_identity(notice,source);row=store.records['gameEvents/'+key]
  row.update(extractionVersion=4,originalLanguage='en',languageEvidence={'status':'verified','language':'en','method':'editorial','sourceSha256':notice['raw_sha256'],'checkedAt':NOW.isoformat(),'evidenceUrl':notice['official_url']})
  upsert_notice(store,source,notice,NOW);children=[r for _,r in store.list('gameEvents')if r.get('announcementKind')=='new'];self.assertEqual(children[0]['originalLanguage'],'en');self.assertEqual(children[0]['languageEvidence']['sourceSha256'],notice['raw_sha256'])
  upsert_notice(store,source,{**notice,'raw_sha256':'changed-parent-raw'},NOW);children=[r for _,r in store.list('gameEvents')if r.get('announcementKind')=='new'];self.assertEqual(children[0]['originalLanguage'],'unknown');self.assertIsNone(children[0]['languageEvidence'])
 def test_explicit_labelled_period_converts_offset(self):
  schedule=extract_schedule(self.period());self.assertEqual(schedule['startsAt'].isoformat(),'2026-10-13T02:00:00+00:00');self.assertEqual(schedule['endsAt'].isoformat(),'2026-10-28T10:59:00+00:00');self.assertEqual(schedule['reasonCodes'],[])
 def test_korean_and_japanese_complete_dates(self):
  for body in ['이벤트 기간: 2026년 10월 13일 10시00분 ~ 2026년 10월 28일 18시59분 (KST)', '開催期間: 2026年10月13日 10:00 ～ 2026年10月28日 18:59 (JST)']:
   self.assertEqual(extract_schedule(body)['startsAt'],datetime(2026,10,13,1,tzinfo=UTC));self.assertEqual(extract_schedule(body)['reasonCodes'],[])
 def test_iso_offsets_and_labelled_boundaries(self):
  schedule=extract_schedule('Starts: 2026-10-13T10:00:00Z\nEnds: 2026-10-28T18:59:00Z');self.assertEqual(schedule['startsAt'],datetime(2026,10,13,10,tzinfo=UTC))
 def test_bare_utc_clock_is_explicit_zero_offset(self):
  self.assertEqual(extract_schedule(self.period().replace('UTC+8','UTC'))['startsAt'],datetime(2026,10,13,10,tzinfo=UTC))
 def test_explicit_english_month_dates_keep_original_evidence(self):
  for text in ['Event Duration: October 13, 2026 10:00 - October 28, 2026 18:59 (UTC+8)','Event Duration: 13 October 2026 10:00 - 28 October 2026 18:59 (UTC+8)']:
   result=extract_schedule(text);self.assertEqual(result['startsAt'],datetime(2026,10,13,2,tzinfo=UTC));self.assertEqual(result['evidenceText'],text)
 def test_twelve_hour_clock_cannot_be_read_as_24_hour_time(self):
  self.assertIsNone(extract_schedule(self.period().replace('10:00','10:00 PM'))['startsAt'])
 def test_publication_timestamp_and_unlabelled_dates_never_schedule(self):
  for text in ['Published: 2026-10-13 10:00 (UTC+8)', '2026-10-13 10:00 - 2026-10-28 18:59 (UTC+8)', 'Event Duration: 10/13 10:00 - 10/28 18:59 (UTC+8)']:
   self.assertIsNone(extract_schedule(text)['startsAt'])
 def test_timezone_missing_conflicting_or_invalid_stays_pending(self):
  for text in [self.period().replace(' (UTC+8)',''), self.period()+' JST',self.period().replace('UTC+8','UTC+8:90'),self.period().replace('UTC+8','UTC+15')]:
   self.assertIsNone(extract_schedule(text)['startsAt'])
 def test_multiple_event_periods_fail_closed(self):
  result=extract_schedule(self.period()+'\nEvent Duration: 2026-10-14 10:00 - 2026-10-29 18:59 (UTC+8)');self.assertIn('SCHEDULE_CONFLICT',result['reasonCodes'])
 def test_invalid_and_descending_date_rejected(self):
  for text in [self.period().replace('2026-10-13','2026-02-30'),self.period().replace('2026-10-28','2026-10-12')]:self.assertIsNone(extract_schedule(text)['startsAt'])
 def test_source_review_is_independent_of_feed_permission(self):
  result=enrich_document(self.notice(self.period()),self.source(schedule_extraction_approved=False));self.assertFalse(result['schedule_verified']);self.assertIn('EXTRACTION_REVIEW_REQUIRED',result['schedule_reason_codes'])
 def test_unapproved_source_and_unreviewed_article_cannot_verify(self):
  self.assertFalse(enrich_document(self.notice(self.period()),self.source(access_status='unknown'))['schedule_verified'])
  self.assertFalse(enrich_document(self.notice(self.period(),official_url='https://unreviewed.example/notice'),self.source())['schedule_verified'])
 def test_unknown_and_competing_server_regions_fail_closed(self):
  for body,region in [(self.period(),'UNSPECIFIED'),(self.period()+'\nJP server','KR'),(self.period()+'\nKR server, JP server','KR')]:self.assertFalse(enrich_document(self.notice(body,server_region=region),self.source())['schedule_verified'])
 def test_exact_tier_character_new_and_rerun_are_distinct(self):
  text='New 5-star character "First" pickup\nReturning 5-star character "Second" pickup\nNew 4-star character "Third" pickup'
  rows=highest_rarity_targets(text,self.source());self.assertEqual([(r['name'],r['announcementKind'])for r in rows],[('First','new'),('Second','rerun')])
 def test_game_specific_ssr_and_s_tier_do_not_assume_global_rarity(self):
  text='신규 SSR 인형 "Myt" 픽업\n복각 S 캐릭터 "Second" 픽업'
  self.assertEqual(highest_rarity_targets(text,self.source()),[])
  source=self.source(highest_character_rarity={'labels':['SSR'],'evidenceUrl':'https://official.example/rarity','checkedAt':NOW.isoformat()})
  self.assertEqual([r['name']for r in highest_rarity_targets(text,source)],['Myt'])
 def test_unknown_rarity_mixed_tier_and_mixed_roles_not_split(self):
  for text in ['New character "First" pickup','New 5-star character "First" returns','New 5-star character "First" and 4-star character "Second"','New 5-star weapon "Sword" pickup']:
   self.assertEqual(highest_rarity_targets(text,self.source()),[])
 def test_rarity_rule_without_official_evidence_is_disabled(self):
  self.assertEqual(highest_rarity_targets('New 5-star character "First" pickup',self.source(highest_character_rarity={'labels':['5-star']})),[])
 def test_character_name_is_not_a_new_or_rerun_role_marker(self):
  self.assertEqual(highest_rarity_targets('5-star character "New" pickup',self.source()),[])
 def test_publisher_image_origin_does_not_grant_reuse(self):
  result=enrich_document(self.notice('[img]https://cdn.example/banner.webp[/img]'),self.source());self.assertIsNone(result['approved_image']);self.assertEqual(result['image_candidates'][0]['rightsStatus'],'UNCONFIRMED')
 def test_reviewed_exact_host_can_allow_image_but_other_host_cannot(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  result=enrich_document(self.notice('<img src="https://cdn.example/banner.webp"><img src="https://evil.cdn.example/banner.webp">'),source)
  self.assertEqual([r['rightsStatus']for r in result['image_candidates']],['APPROVED','UNCONFIRMED']);self.assertIsNone(result['approved_image'])
 def test_reuse_permission_without_subject_evidence_never_displays_first_art(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  result=enrich_document(self.notice('<img src="https://cdn.example/another-game-banner.webp">'),source)
  self.assertEqual(result['image_candidates'][0]['rightsStatus'],'APPROVED');self.assertEqual(result['image_candidates'][0]['subjectStatus'],'UNCONFIRMED');self.assertIsNone(result['approved_image'])
 def test_image_alt_must_match_this_notice_not_different_event(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  notice=self.notice('<img src="https://cdn.example/another.webp" alt="Unrelated event"><img src="https://cdn.example/current.webp" alt="Official pickup">')
  row=enrich_document(notice,source);self.assertEqual(row['approved_image']['url'],'https://cdn.example/current.webp');self.assertEqual(row['approved_image']['subjectEvidence']['eventId'],event_identity(notice,source))
 def test_catalog_logo_and_icon_cannot_be_event_artwork_even_with_matching_alt(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  for name in ('logo.png','app-icon.webp','key-visual.jpg','game-cover.png'):
   row=enrich_document(self.notice('<img src="https://cdn.example/'+name+'" alt="Official pickup">'),source);self.assertIsNone(row['approved_image'])
 def test_two_matching_images_require_review_instead_of_first_image_selection(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  row=enrich_document(self.notice('<img src="https://cdn.example/one.webp" alt="Official pickup"><img src="https://cdn.example/two.webp" alt="Official pickup">'),source);self.assertIsNone(row['approved_image'])
 def test_matched_image_still_requires_separate_reuse_review(self):
  row=enrich_document(self.notice('<img src="https://cdn.example/current.webp" alt="Official pickup">'),self.source());self.assertEqual(row['image_candidates'][0]['subjectStatus'],'MATCHED');self.assertEqual(row['image_candidates'][0]['rightsStatus'],'UNCONFIRMED');self.assertIsNone(row['approved_image'])
 def test_unreviewed_source_cannot_supply_image_subject_evidence(self):
  row=enrich_document(self.notice('<img src="https://cdn.example/current.webp" alt="Official pickup">'),self.source(access_status='unknown'));self.assertIsNone(row['image_candidates'][0]['subjectEvidence'])
 def test_youtube_thumbnail_not_promoted_to_pickup_banner_by_host_reuse_policy(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['i.ytimg.com'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  row=enrich_document(self.notice(self.period(),source_image_urls=['https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg']),source);self.assertIsNone(row['approved_image'])
 def test_new_and_rerun_children_receive_only_own_labelled_image(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  body=self.period()+'\nNew 5-star character "First" pickup\n<img src="https://cdn.example/first.webp" alt="New First pickup banner">\nReturning 5-star character "Second" pickup\n<img src="https://cdn.example/second.webp" alt="Returning Second pickup banner">'
  store=MemoryStore();upsert_notice(store,source,self.notice(body),NOW)
  children={row['announcementKind']:row for _,row in store.list('gameEvents')if row.get('announcementKind')in('new','rerun')}
  self.assertEqual(children['new']['image']['url'],'https://cdn.example/first.webp');self.assertEqual(children['rerun']['image']['url'],'https://cdn.example/second.webp')
  for row in children.values():self.assertEqual(row['image']['subjectEvidence']['eventId'],row['id']);self.assertEqual(row['image']['subjectEvidence']['announcementKind'],row['announcementKind'])
 def test_generic_parent_art_not_copied_to_new_or_rerun_children(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  body='<img src="https://cdn.example/post.webp" alt="Official pickup">\n'+self.period()+'\nNew 5-star character "First" pickup\nReturning 5-star character "Second" pickup'
  store=MemoryStore();upsert_notice(store,source,self.notice(body),NOW)
  self.assertTrue(all(row.get('image')is None for _,row in store.list('gameEvents')if row.get('announcementKind')in('new','rerun')))
 def test_role_mismatched_character_banner_not_attached_to_child(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  body=self.period()+'\nNew 5-star character "First" pickup\n<img src="https://cdn.example/first.webp" alt="Returning First pickup banner">'
  store=MemoryStore();upsert_notice(store,source,self.notice(body),NOW);row=[r for _,r in store.list('gameEvents')if r.get('announcementKind')=='new'][0];self.assertIsNone(row['image'])
 def test_wrong_character_alt_not_overridden_by_correct_nearby_paragraph(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  body=self.period()+'\nNew 5-star character "First" pickup\n<img src="https://cdn.example/another.webp" alt="New Second pickup banner">'
  store=MemoryStore();upsert_notice(store,source,self.notice(body),NOW);row=[r for _,r in store.list('gameEvents')if r.get('announcementKind')=='new'][0];self.assertIsNone(row['image'])
 def test_character_name_substring_is_not_subject_match(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  body=self.period()+'\nNew 5-star character "First" pickup\n<img src="https://cdn.example/another.webp" alt="New Firstborn pickup banner">'
  store=MemoryStore();upsert_notice(store,source,self.notice(body),NOW);row=[r for _,r in store.list('gameEvents')if r.get('announcementKind')=='new'][0];self.assertIsNone(row['image'])
 def test_extraction_version_reverify_removes_legacy_approved_wrong_image(self):
  source=self.source();notice=self.notice(self.period());key=event_identity(notice,source)
  store=MemoryStore({'gameEvents/'+key:{'rawSha256':notice['raw_sha256'],'extractionVersion':4,'image':{'url':'https://cdn.example/wrong.webp','rightsStatus':'APPROVED'},'scheduleOrigin':'automatic'}})
  upsert_notice(store,source,notice,NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['extractionVersion'],EXTRACTION_VERSION);self.assertIsNone(row['image'])
 def test_unsafe_image_candidates_are_not_preserved(self):
  result=enrich_document(self.notice('[img]https://127.0.0.1/banner.webp[/img][img]https://localhost/banner.webp[/img]'),self.source());self.assertEqual(result['image_candidates'],[])
 def test_verified_schedule_saved_only_with_source_review_and_region(self):
  store=MemoryStore();upsert_notice(store,self.source(),self.notice(self.period()),NOW);row=store.list('gameEvents')[0][1];self.assertEqual(row['verificationStatus'],'verified');self.assertEqual(row['startsAt'],datetime(2026,10,13,2,tzinfo=UTC));self.assertEqual(row['publishedAt'],NOW)
 def test_manual_date_conflict_preserves_reviewed_value_and_revokes(self):
  store=MemoryStore();notice=self.notice(self.period());key=event_identity(notice,self.source());store.records['gameEvents/'+key]={'startsAt':NOW,'verificationStatus':'verified','originalTimezone':'UTC'}
  upsert_notice(store,self.source(),notice,NOW);row=store.get('gameEvents/'+key);self.assertEqual(row['startsAt'],NOW);self.assertEqual(row['verificationStatus'],'pending');self.assertIn('MANUAL_SCHEDULE_CONFLICT',row['scheduleEvidence']['reasonCodes'])
 def test_child_posts_preserve_original_notice_and_separate_main_rerun(self):
  store=MemoryStore();notice=self.notice(self.period()+'\nNew 5-star character "First" pickup\nReturning 5-star character "Second" pickup');upsert_notice(store,self.source(),notice,NOW)
  rows=[row for _,row in store.list('gameEvents')if row.get('parentEventId')];self.assertEqual(len(rows),2);self.assertEqual({r['announcementKind']for r in rows},{'new','rerun'});self.assertTrue(all(r['originalTitle']==notice['original_title']for r in rows));self.assertTrue(all(r['verificationStatus']=='verified'for r in rows))
 def test_enriched_upsert_unchanged_data_has_zero_extra_writes(self):
  store=MemoryStore();notice=self.notice(self.period()+'\nNew 5-star character "First" pickup');upsert_notice(store,self.source(),notice,NOW);writes=store.writes;self.assertEqual(upsert_notice(store,self.source(),notice,NOW),'unchanged');self.assertEqual(store.writes,writes)
 def test_removed_child_is_cancelled_and_cannot_remain_calendar_verified(self):
  store=MemoryStore();source=self.source();notice=self.notice(self.period()+'\nNew 5-star character "First" pickup');upsert_notice(store,source,notice,NOW);old=[row for _,row in store.list('gameEvents')if row.get('parentEventId')][0]
  upsert_notice(store,source,{**self.notice(self.period()),'raw_sha256':'revised'},NOW);row=store.get('gameEvents/'+old['id']);self.assertEqual(row['status'],'cancelled');self.assertEqual(row['verificationStatus'],'pending');self.assertTrue(any(r.get('status')=='cancelled'for _,r in store.list('pickupSchedules')))
 def test_automatic_date_removed_on_unconfirmed_correction(self):
  store=MemoryStore();source=self.source();notice=self.notice(self.period());upsert_notice(store,source,notice,NOW);upsert_notice(store,source,{**self.notice('Schedule changed; dates TBA'),'raw_sha256':'correction'},NOW);row=store.get('gameEvents/'+event_identity(notice,source));self.assertIsNone(row['startsAt']);self.assertEqual(row['verificationStatus'],'pending')
 def test_explicit_cancellation_revokes_active_calendar_status(self):
  store=MemoryStore();upsert_notice(store,self.source(),self.notice(self.period()+'\nEvent has been cancelled'),NOW);row=store.list('gameEvents')[0][1];self.assertEqual(row['status'],'cancelled')
 def test_oversized_public_record_fails_before_event_write(self):
  store=MemoryStore();notice=self.notice('字'*160000)
  with self.assertRaisesRegex(SafetyError,'event_document_exceeds'):upsert_notice(store,self.source(),notice,NOW)
  self.assertEqual(store.list('gameEvents'),[])
 def test_source_policy_roundtrip_and_reseed_preserves_admin_review(self):
  source=self.source();row=source_row(source,'blue-archive');self.assertEqual(operating_source(row)['highest_character_rarity'],source['highest_character_rarity'])
  store=MemoryStore({'gameSources/source':row});seed_catalogue(store,[],{'games':[{'gameId':'blue-archive','sources':[SOURCE]}]});self.assertTrue(store.get('gameSources/source')['scheduleExtractionApproved'])
 def test_admin_rejects_extraction_without_region_or_evidence(self):
  row=source_row(SOURCE,'blue-archive');row.update(scheduleExtractionApproved=True)
  with self.assertRaisesRegex(SafetyError,'schedule_extraction_review'):validate_change('gameSources','source',row)
 def test_admin_rejects_unreviewed_rarity_and_image_rights(self):
  for changes,reason in [({'highestCharacterRarity':{'labels':['SSR']}},'rarity_evidence'),({'imageReuse':{'rightsStatus':'APPROVED','allowedHosts':['cdn.example']}},'image_reuse_evidence')]:
   with self.assertRaisesRegex(SafetyError,reason):validate_change('gameSources','source',{**source_row(SOURCE,'blue-archive'),**changes})

class FirebasePublisherSectionTests(unittest.TestCase):
 # Publisher heading syntax and dates are factual; descriptions are deliberately
 # minimal test metadata. No full publisher article/image is packaged publicly.
 BODY='''Time-Limited Events
■[Amber Reel of Moonlight]
▸Event Duration:
2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)

■[Inverse Cipher]
▸Event Duration:
2026/10/14 05:00 ~ 2026/10/28 04:59 (GMT+0)

■[Intelligence Supplies]
Log in during the event.
▸Event Duration:
2026/10/16 05:00 ~ 2026/10/21 04:59 (GMT+0)

Recruitment
■Targeted Procurement Event | Mityl
Elite Doll [Mityl]
■Military Upgrade | Squirrel Nova
Elite Weapon [Squirrel Nova]
▸Duration:
2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)

■Targeted Procurement Event | Cheyanne
Elite Doll [Cheyanne]
■Military Upgrade | Nightwalker Cardamom
Elite Weapon [Nightwalker Cardamom]
▸Duration:
2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)

■Mityl' Outfit | 90s' Sunset Memories
Brand-new outfit.
▸Discount Duration:
2026/10/8 9:00 - 2026/10/28 18:59(GMT+0)'''
 CLOCK=datetime(2026,10,13,7,tzinfo=UTC)
 def source(self,**changes):
  return {**SOURCE,'server_region':'GLOBAL','server_label':'HAOPLAY Steam','schedule_extraction_approved':True,
   'schedule_extraction_reviewed_at':self.CLOCK.isoformat(),'schedule_extraction_evidence_url':'https://steamcommunity.com/announcement/1',
   'section_schedule_format':'heading_period_v1','paired_recruitment_period_approved':True,**changes}
 def notice(self,body=None):
  return {**document(region='GLOBAL'),'original_title':'Publisher update overview','original_description':body or self.BODY,
   'raw_json':{'contents':body or self.BODY},'raw_sha256':'section-fixture-'+str(len(body or self.BODY)),
   'published_at':'2026-10-07T10:00:48+00:00','event_type_candidates':['update'],'pickups':[]}
 def test_period_label_on_immediate_next_line_has_explicit_dates(self):
  result=extract_schedule('▸Duration:\n2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)');self.assertEqual(result['startsAt'],datetime(2026,10,8,9,tzinfo=UTC));self.assertEqual(result['reasonCodes'],[])
 def test_isolated_section_image_matches_only_its_own_event(self):
  body=self.BODY.replace('■[Inverse Cipher]','[img]https://cdn.example/amber.webp[/img]\n\n■[Inverse Cipher]')
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  sections=section_documents(self.notice(body),source)
  first=enrich_document(sections[0],source);second=enrich_document(sections[1],source)
  self.assertEqual(first['approved_image']['url'],'https://cdn.example/amber.webp');self.assertEqual(first['approved_image']['subjectEvidence']['kind'],'event');self.assertIsNone(second['approved_image'])
 def test_parent_feed_images_never_inherited_by_section(self):
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  sections=section_documents({**self.notice(),'source_image_urls':['https://cdn.example/parent.webp']},source)
  self.assertTrue(all(enrich_document(section,source)['image_candidates']==[] for section in sections))
 def test_multiple_section_images_without_local_labels_not_first_image(self):
  body=self.BODY.replace('■[Inverse Cipher]','[img]https://cdn.example/one.webp[/img]\n[img]https://cdn.example/two.webp[/img]\n\n■[Inverse Cipher]')
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  first=section_documents(self.notice(body),source)[0];self.assertIsNone(enrich_document(first,source)['approved_image'])
 def test_isolated_heading_cannot_override_explicit_unrelated_image_alt(self):
  body=self.BODY.replace('■[Inverse Cipher]','<img src="https://cdn.example/another.webp" alt="Unrelated poster">\n\n■[Inverse Cipher]')
  source=self.source(image_reuse={'rightsStatus':'APPROVED','allowedHosts':['cdn.example'],'evidenceUrl':'https://official.example/reuse','checkedAt':NOW.isoformat()})
  first=section_documents(self.notice(body),source)[0];self.assertIsNone(enrich_document(first,source)['approved_image'])
 def test_multiple_heading_sections_have_independent_periods(self):
  document=self.notice();source=self.source();parts=section_documents(document,source);self.assertEqual(len(parts),8)
  self.assertEqual(parts[0]['_section_title'],'Amber Reel of Moonlight');self.assertEqual(enrich_document(parts[0],source)['schedule']['startsAt'],datetime(2026,10,8,9,tzinfo=UTC))
  self.assertEqual(enrich_document(parts[1],source)['schedule']['startsAt'],datetime(2026,10,14,5,tzinfo=UTC));self.assertFalse(enrich_document(document,source)['schedule_verified'])
 def test_character_weapon_pair_requires_specific_format_review(self):
  source=self.source(paired_recruitment_period_approved=False);parts=section_documents(self.notice(),source)
  self.assertFalse(enrich_document(parts[3],source)['schedule_verified']);self.assertTrue(enrich_document(parts[4],source)['schedule_verified'])
 def test_verified_pair_has_factual_character_and_weapon_targets(self):
  source=self.source();parts=section_documents(self.notice(),source);self.assertEqual(parts[3]['pickups'][0]['characters'],['Mityl']);self.assertEqual(parts[4]['pickups'][0]['weapons'],['Squirrel Nova'])
  self.assertTrue(enrich_document(parts[3],source)['schedule_verified']);self.assertEqual(enrich_document(parts[3],source)['highest_rarity_targets'],[])
 def test_new_outfit_does_not_mark_adjacent_character_new(self):
  source=self.source(highest_character_rarity={'labels':['Elite'],'evidenceUrl':'https://official.example/rarity','checkedAt':self.CLOCK.isoformat()})
  result=enrich_document(self.notice(),source);self.assertEqual(result['highest_rarity_targets'],[])
  self.assertTrue(all(not enrich_document(section,source)['highest_rarity_targets']for section in result['section_documents']))
 def test_future_explicit_elite_doll_role_can_split_when_rarity_reviewed(self):
  source=self.source(highest_character_rarity={'labels':['Elite'],'evidenceUrl':'https://official.example/rarity','checkedAt':self.CLOCK.isoformat()})
  self.assertEqual(highest_rarity_targets('New Elite Doll [Mityl]',source)[0]['announcementKind'],'new');self.assertEqual(highest_rarity_targets('Returning Elite Doll [Cheyanne]',source)[0]['announcementKind'],'rerun')
 def test_section_events_keep_parent_original_and_independent_canonical_ids(self):
  store=MemoryStore();notice=self.notice();source=self.source();upsert_notice(store,source,notice,self.CLOCK);rows=store.list('gameEvents')
  children=[row for _,row in rows if row.get('sourceSectionKey')];self.assertEqual(len(children),8);self.assertEqual(len({row['id']for row in children}),8)
  self.assertTrue(all(row['originalTitle']==notice['original_title'] and row['originalDescription']==notice['original_description']for row in children))
  self.assertTrue(all(row['verificationStatus']=='verified'for row in children));self.assertEqual(len(store.list('collectionDocuments')),1)
  pickups=[row for _,row in store.list('pickupSchedules')];self.assertEqual(len(pickups),4);self.assertTrue(all(row['verificationStatus']=='verified'for row in pickups))
 def test_section_unchanged_revalidation_has_zero_writes(self):
  store=MemoryStore();notice=self.notice();source=self.source();upsert_notice(store,source,notice,self.CLOCK);before=store.writes;upsert_notice(store,source,notice,self.CLOCK+timedelta(minutes=15));self.assertEqual(store.writes,before)
 def test_true_conflict_within_one_section_stays_pending(self):
  text='■[Conflicting event]\nEvent Duration: 2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)\nEvent Duration: 2026/10/9 9:00 ~ 2026/10/29 18:59 (GMT+0)'
  source=self.source();part=section_documents(self.notice(text),source)[0];self.assertIn('SCHEDULE_CONFLICT',enrich_document(part,source)['schedule_reason_codes'])
 def test_multi_phase_section_cannot_use_first_subperiod_as_whole_event(self):
  text='■[Multi phase event]\n▸Event Duration:\nRecruitment Period: 2026/10/8 9:00 ~ 2026/10/9 18:59 (GMT+0)\nVictory Period: 2026/10/14 5:00 ~ 2026/10/28 18:59 (GMT+0)'
  source=self.source();part=section_documents(self.notice(text),source)[0];self.assertFalse(enrich_document(part,source)['schedule_verified'])
 def test_old_published_notice_can_supply_confirmed_ongoing_period(self):
  self.assertTrue(confirmed_window_overlap(self.notice(),self.source(),self.CLOCK-timedelta(hours=72),self.CLOCK));self.assertFalse(confirmed_window_overlap(self.notice(),self.source(schedule_extraction_approved=False),self.CLOCK-timedelta(hours=72),self.CLOCK))
 def test_bounded_collector_includes_old_ongoing_but_not_expired(self):
  source=self.source();raw=json.loads(response(title='Publisher update overview',date=datetime(2026,10,7,10,tzinfo=UTC)));raw['appnews']['newsitems'][0]['contents']=self.BODY
  store=MemoryStore();result=collect_one(store,source,self.CLOCK-timedelta(hours=72),self.CLOCK,mode='backfill',fetch=lambda _:(200,json.dumps(raw).encode(),{}),sleep=lambda _:None,now=self.CLOCK)
  self.assertEqual(result['status'],'success');self.assertEqual(result['metrics']['ongoingOlderIncluded'],1);self.assertEqual(result['metrics']['verifiedSectionSchedules'],8);self.assertEqual(len([row for _,row in store.list('gameEvents')if row.get('sourceSectionKey')]),8)
  expired=self.BODY.replace('2026/10/28','2026/10/9').replace('2026/10/21','2026/10/9')
  self.assertFalse(confirmed_window_overlap(self.notice(expired),source,self.CLOCK-timedelta(hours=72),self.CLOCK))
 def test_heading_removal_cancels_its_old_pickup(self):
  source=self.source();store=MemoryStore();notice=self.notice();upsert_notice(store,source,notice,self.CLOCK)
  old=[row for _,row in store.list('gameEvents')if row.get('sourceSectionTitle')=='Targeted Procurement Event | Mityl'][0]
  changed=self.BODY.replace('■Targeted Procurement Event | Mityl\nElite Doll [Mityl]\n','');upsert_notice(store,source,self.notice(changed),self.CLOCK)
  self.assertEqual(store.get('gameEvents/'+old['id'])['status'],'cancelled');self.assertTrue(all(row['status']=='cancelled'for _,row in store.list_equal('pickupSchedules','eventId',old['id'],50)))
 def test_bounded_sections_do_not_enable_unreviewed_splitting(self):
  self.assertEqual(section_documents(self.notice(),self.source(section_schedule_format=None)),[])
 def test_section_canonical_id_does_not_change_when_other_heading_added(self):
  source=self.source();before=section_documents(self.notice(),source);after=section_documents(self.notice('■[Other]\nNo dates\n'+self.BODY),source)
  self.assertEqual(event_identity(before[0],source),event_identity(after[1],source))
 def test_bbcode_headings_preserve_distinct_section_boundaries(self):
  text=self.BODY.replace('■[Amber Reel of Moonlight]','[h2]Amber Reel of Moonlight[/h2]');self.assertEqual(section_documents(self.notice(text),self.source())[0]['_section_title'],'Amber Reel of Moonlight')
 def test_next_heading_cannot_supply_previous_unpaired_period(self):
  text='■[No period]\n▸Event Duration:\n■[Different event]\n▸Event Duration:\n2026/10/8 9:00 ~ 2026/10/28 18:59 (GMT+0)'
  source=self.source();parts=section_documents(self.notice(text),source);self.assertFalse(enrich_document(parts[0],source)['schedule_verified']);self.assertTrue(enrich_document(parts[1],source)['schedule_verified'])
 def test_period_passage_updates_status_without_rewriting_raw(self):
  source=self.source();store=MemoryStore();notice=self.notice();upsert_notice(store,source,notice,self.CLOCK);raw_writes=len(store.list('collectionDocuments'))
  upsert_notice(store,source,notice,datetime(2026,10,29,0,tzinfo=UTC));rows=[row for _,row in store.list('gameEvents')if row.get('sourceSectionKey')];self.assertTrue(all(row['status']=='ended'for row in rows));self.assertEqual(len(store.list('collectionDocuments')),raw_writes)
 def test_period_review_can_keep_source_region_explicitly_unknown(self):
  row=source_row(self.source(server_region='UNSPECIFIED'),'blue-archive');validated=validate_change('gameSources','source',row);self.assertEqual(validated['serverRegion'],'UNSPECIFIED');self.assertTrue(validated['scheduleExtractionApproved'])
 def test_unknown_region_source_dates_remain_candidate_only(self):
  source=self.source(server_region='UNSPECIFIED');notice={**self.notice(),'server_region':'UNSPECIFIED'};store=MemoryStore();upsert_notice(store,source,notice,self.CLOCK)
  rows=[row for _,row in store.list('gameEvents')if row.get('sourceSectionKey')];self.assertEqual(len(rows),8)
  self.assertTrue(all(row['verificationStatus']=='pending' and row['startsAt'] is None and row['endsAt'] is None for row in rows))
  self.assertTrue(all(row['candidateStartsAt'] and row['candidateEndsAt'] and row['candidateTimezone']=='UTC'for row in rows))
  pickups=[row for _,row in store.list('pickupSchedules')];self.assertTrue(all(row['verificationStatus']=='pending' and row['startsAt'] is None and row['candidateStartsAt']for row in pickups))
 def test_older_unknown_region_period_is_ingested_pending_not_verified(self):
  source=self.source(server_region='UNSPECIFIED');raw=json.loads(response(title='Publisher update overview',date=datetime(2026,10,7,10,tzinfo=UTC)));raw['appnews']['newsitems'][0]['contents']=self.BODY
  store=MemoryStore();result=collect_one(store,source,self.CLOCK-timedelta(hours=72),self.CLOCK,mode='backfill',fetch=lambda _:(200,json.dumps(raw).encode(),{}),sleep=lambda _:None,now=self.CLOCK)
  self.assertEqual(result['status'],'success');self.assertEqual(result['metrics']['ongoingOlderIncluded'],1);self.assertEqual(result['metrics']['verifiedSchedules'],0);self.assertEqual(result['metrics']['candidateSectionSchedules'],8)
 def test_unknown_region_dates_never_create_remote_notification_jobs(self):
  source=self.source(server_region='UNSPECIFIED');notice={**self.notice(),'server_region':'UNSPECIFIED'};store,_=owner_store();store.records.pop('gameEvents/event');upsert_notice(store,source,notice,self.CLOCK)
  rows=[row for _,row in store.list('gameEvents')if row.get('sourceSectionKey')]
  self.assertTrue(all(not eligible(store,'alice',row,'event:start',self.CLOCK)for row in rows));self.assertEqual(plan_user(store,'alice',rows,self.CLOCK),0);self.assertEqual(store.list('users/alice/notificationJobs'),[])
 def test_missing_approval_or_timezone_cannot_use_candidate_window_path(self):
  notice={**self.notice(),'server_region':'UNSPECIFIED'}
  self.assertFalse(confirmed_window_overlap(notice,self.source(server_region='UNSPECIFIED',schedule_extraction_approved=False),self.CLOCK-timedelta(hours=72),self.CLOCK))
  missing_zone={**notice,'original_description':notice['original_description'].replace('(GMT+0)','')}
  self.assertFalse(confirmed_window_overlap(missing_zone,self.source(server_region='UNSPECIFIED'),self.CLOCK-timedelta(hours=72),self.CLOCK))

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

class FirebaseLocalizedEventPayloadTests(unittest.TestCase):
 def fixture(self,lang='ko',**changes):
  store,event=owner_store();store.records['users/alice']['language']=lang
  event={**event,'originalTitle':'Synthetic official pickup','originalDescription':'New synthetic character pickup details.','originalLanguage':'en','rawSha256':'fixture-raw-hash','revision':3,**changes}
  return store,event
 def translation(self,lang='ko',**changes):
  text={'ko':('합성 캐릭터 픽업 안내','합성 캐릭터의 픽업 일정 안내입니다.'),'ja':('合成キャラクターピックアップのお知らせ','合成キャラクターのピックアップ日程をご確認ください。'),'en':('Synthetic translated pickup notice','Synthetic translated pickup schedule details.')}
  title,body=text[lang]
  return {'title':title,'description':body,'status':'completed','verificationStatus':'verified','sourceLanguage':'en','targetLanguage':lang,'sourceSha256':'fixture-raw-hash','sourceTitle':'Synthetic official pickup','sourceRevision':3,**changes}
 def payload(self,store,event,**job):return fixed_payload(store,'alice',{'id':'job','kind':'event:start',**job},event,NOW)
 def test_verified_korean_event_translation_used_in_notification(self):
  s,e=self.fixture(translations={'ko':self.translation()});p=self.payload(s,e);self.assertEqual(p['title'],'합성 캐릭터 픽업 안내');self.assertEqual(p['body'],'합성 캐릭터의 픽업 일정 안내입니다.')
 def test_untranslated_english_source_not_korean_push_fallback(self):
  s,e=self.fixture();p=self.payload(s,e);self.assertEqual(p['title'],'캐릭터·무기 픽업 소식');self.assertEqual(p['body'],'자세한 내용은 원문 공식 공지에서 확인해 주세요.');self.assertNotIn('Synthetic',p['title']+p['body'])
 def test_untranslated_korean_source_not_japanese_or_english_push_fallback(self):
  for lang in ('ja','en'):
   s,e=self.fixture(lang,originalLanguage='ko',originalTitle='합성 한국어 공식 공지',originalDescription='합성 한국어 본문입니다.');p=self.payload(s,e);self.assertNotIn('합성',p['title']+p['body'])
 def test_verified_japanese_translation_uses_japanese_notice_text(self):
  s,e=self.fixture('ja',translations={'ja':self.translation('ja')});p=self.payload(s,e);self.assertEqual(p['title'],'合成キャラクターピックアップのお知らせ');self.assertIn('日程',p['body'])
 def test_selected_locale_original_text_used_only_when_script_matches(self):
  for lang,title,body in [('ko','합성 한국어 픽업','합성 한국어 본문 안내입니다.'),('ja','合成ピックアップ','合成の日程をご確認ください。'),('en','Synthetic pickup','Synthetic source details.')]:
   s,e=self.fixture(lang,originalLanguage=lang,originalTitle=title,originalDescription=body);p=self.payload(s,e);self.assertEqual(p['title'],title);self.assertEqual(p['body'],body)
 def test_incorrect_korean_source_language_tag_cannot_pass_english_original(self):
  s,e=self.fixture(originalLanguage='ko');p=self.payload(s,e);self.assertEqual(p['title'],'캐릭터·무기 픽업 소식');self.assertNotIn('New synthetic',p['body'])
 def test_copied_english_translation_does_not_pass_korean_or_japanese_locale(self):
  for lang in ('ko','ja'):
   s,e=self.fixture(lang);e['translations']={lang:self.translation(lang,title=e['originalTitle'],description=e['originalDescription'])};p=self.payload(s,e);self.assertNotIn('Synthetic',p['title']);self.assertNotIn('New synthetic',p['body'])
 def test_korean_translation_body_cannot_leak_english_when_title_valid(self):
  s,e=self.fixture(translations={'ko':self.translation(description='New synthetic character pickup details.')});p=self.payload(s,e);self.assertEqual(p['title'],'합성 캐릭터 픽업 안내');self.assertEqual(p['body'],'자세한 내용은 원문 공식 공지에서 확인해 주세요.')
 def test_unverified_rejected_or_failed_translation_not_used(self):
  for patch in ({'status':'pending'},{'status':'failed'},{'verificationStatus':'pending'},{'verificationStatus':'rejected'}):
   s,e=self.fixture(translations={'ko':self.translation(**patch)});self.assertEqual(self.payload(s,e)['title'],'캐릭터·무기 픽업 소식')
 def test_target_or_source_language_tag_mismatch_blocks_translation(self):
  for patch in ({'targetLanguage':'ja'},{'sourceLanguage':'ja'}):
   s,e=self.fixture(translations={'ko':self.translation(**patch)});self.assertEqual(self.payload(s,e)['title'],'캐릭터·무기 픽업 소식')
 def test_stale_source_hash_or_different_section_title_blocks_translation(self):
  for patch in ({'sourceSha256':'changed-source-hash'},{'sourceTitle':'Synthetic other pickup'}):
   s,e=self.fixture(translations={'ko':self.translation(**patch)});self.assertEqual(self.payload(s,e)['title'],'캐릭터·무기 픽업 소식')
 def test_extraction_only_revision_increment_keeps_exact_hash_and_selected_title_translation(self):
  s,e=self.fixture(revision=4,translations={'ko':self.translation(sourceRevision=3)});self.assertEqual(self.payload(s,e)['title'],'합성 캐릭터 픽업 안내')
 def test_changed_revision_without_exact_hash_and_source_title_requires_new_translation(self):
  s,e=self.fixture(revision=4,translations={'ko':self.translation(sourceRevision=3,sourceSha256=None)});self.assertEqual(self.payload(s,e)['title'],'캐릭터·무기 픽업 소식')
 def test_selected_publisher_section_title_is_translation_source_not_parent_title(self):
  s,e=self.fixture(displayTitle='Synthetic selected pickup',translations={'ko':self.translation(sourceTitle='Synthetic selected pickup')});self.assertEqual(self.payload(s,e)['title'],'합성 캐릭터 픽업 안내')
  e['translations']['ko']['sourceTitle']=e['originalTitle'];self.assertEqual(self.payload(s,e)['title'],'캐릭터·무기 픽업 소식')
 def test_notification_markup_and_image_urls_not_rendered_as_message_body(self):
  s,e=self.fixture(translations={'ko':self.translation(title='[b]합성 픽업[/b]',description='<script>bad()</script>[p]합성 안내입니다.[/p][img]https://cdn.example/fixture.webp[/img]')});p=self.payload(s,e);self.assertEqual(p['title'],'합성 픽업');self.assertEqual(p['body'],'합성 안내입니다.')
 def test_localized_event_send_preserves_recipient_url_tag_kind_and_dedupe_without_real_provider(self):
  s,e=self.fixture(translations={'ko':self.translation()});s.records['gameEvents/event']=e;s.put('users/alice/notificationJobs/localized',{'uid':'alice','kind':'event:start','eventId':'event','eventRevision':3,'dedupeKey':'same-key','status':'requested','dueAt':NOW});calls=[]
  self.assertEqual(dispatch(s,'alice','localized',lambda token,payload:calls.append(payload) or 'mock-provider-id',NOW),'provider_accepted');self.assertEqual(len(calls),1);p=calls[0]
  self.assertEqual(p['title'],'합성 캐릭터 픽업 안내');self.assertEqual(p['recipientUid'],'alice');self.assertEqual(p['url'],'/events/event');self.assertEqual(p['tag'],'orbit-event-same-key');self.assertEqual(p['kind'],'event:start');self.assertEqual(p['gameIds'],['blue-archive'])
  self.assertEqual(dispatch(s,'alice','localized',lambda *_:self.fail('duplicate mock send'),NOW),'skipped')
 def test_existing_push_test_localized_message_unchanged(self):
  expected={'ko':('푸시 알림 테스트','이 알림이 보이면 앱에서 수신 확인을 눌러 주세요.'),'en':('Push notification test','If this notification appears, confirm receipt in the app.'),'ja':('プッシュ通知テスト','この通知が表示されたら、アプリで受信を確認してください。')}
  for lang,(title,body)in expected.items():
   s,e=self.fixture(lang);p=fixed_payload(s,'alice',{'id':'test','kind':'push_test'},None,NOW);self.assertEqual((p['title'],p['body']),(title,body));self.assertEqual(p['tag'],'orbit-test-test');self.assertEqual(p['recipientUid'],'alice')

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

class AutomaticReviewedLocalizationTests(unittest.TestCase):
 def source(self,**changes):
  evidence='https://official.example/paired-roster'
  terms=[{'kind':'character','sourceName':'Mityl','names':{'ko':'미틸','ja':'ミティル'},'checkedAt':NOW.isoformat(),'evidenceUrl':evidence},
   {'kind':'weapon','sourceName':'Synthetic Weapon','names':{'ko':'검증 무기'},'checkedAt':NOW.isoformat(),'evidenceUrl':evidence},
   {'kind':'event','sourceName':'Synthetic Festival','names':{'ko':'검증 축제'},'checkedAt':NOW.isoformat(),'evidenceUrl':evidence}]
  return {**SOURCE,'content_language':'en','content_localization':{'mode':'reviewed_templates_v1','version':1,'gameId':SOURCE['game_id'],
   'sourceLanguage':'en','reviewedAt':NOW.isoformat(),'evidenceUrl':evidence,'terms':terms,'formats':['gf2_targeted_procurement_v1','gf2_military_upgrade_v1']},**changes}
 def notice(self,name='Mityl',**changes):
  return {**document('Targeted Procurement Event | '+name), 'original_description':'The drop rate for the Elite Doll ['+name+'] is increased in the Targeted Procurement!\nGenerous rewards are available.',**changes}
 def event(self,source=None,notice=None):
  source=source or self.source();notice=notice or self.notice();store=MemoryStore();upsert_notice(store,source,notice,NOW)
  return store,store.get('gameEvents/'+event_identity(notice,source))
 def test_known_official_names_generate_korean_and_japanese_without_boilerplate(self):
  store,row=self.event();self.assertEqual(row['translations']['ko']['title'],'미틸 · 캐릭터 픽업');self.assertEqual(row['translations']['ja']['title'],'ミティル · キャラクターピックアップ')
  self.assertEqual(row['translations']['ko']['description'],'');self.assertEqual(row['translations']['ko']['sourceTitle'],row['originalTitle']);self.assertEqual(row['translations']['ko']['sourceSha256'],row['rawSha256'])
  self.assertEqual(row['contentLocalization']['languages'],['ko','ja']);self.assertEqual(row['originalDescription'],self.notice()['original_description'])
  self.assertEqual(store.list('pickupSchedules')[0][1]['translations']['ko'],row['translations']['ko'])
 def test_unknown_character_name_is_not_guessed_or_mislabeled_translated(self):
  _,row=self.event(notice=self.notice('Unreviewed Character'));self.assertEqual(row['translations'],{});self.assertIsNone(row['contentLocalization'])
 def test_character_title_cannot_use_a_different_body_target(self):
  _,row=self.event(notice=self.notice(original_description='The drop rate for the Elite Doll [Different] is increased'));self.assertEqual(row['translations'],{})
 def test_dictionary_review_and_game_binding_are_required(self):
  for patch in ({'gameId':'different-game'},{'reviewedAt':'unknown'},{'evidenceUrl':'http://127.0.0.1/'},{'mode':'free_guess'}):
   source=self.source();source['content_localization'].update(patch);_,row=self.event(source);self.assertEqual(row['translations'],{})
 def test_unreviewed_source_cannot_activate_automatic_localization(self):
  _,row=self.event(self.source(access_status='unknown'));self.assertEqual(row['translations'],{})
 def test_dictionary_revision_reprocesses_unchanged_source_and_then_deduplicates(self):
  source=self.source();notice=self.notice();store,row=self.event(source,notice);before=store.writes
  self.assertEqual(upsert_notice(store,source,notice,NOW),'unchanged');self.assertEqual(store.writes,before)
  source['content_localization']['version']=2;source['content_localization']['terms'][0]['names']['ko']='새 검증 이름'
  self.assertEqual(upsert_notice(store,source,notice,NOW),'updated');changed=store.get('gameEvents/'+row['id'])
  self.assertEqual(changed['translations']['ko']['title'],'새 검증 이름 · 캐릭터 픽업');self.assertEqual(changed['contentLocalization']['policyVersion'],2)
 def test_changed_unknown_source_revokes_stale_previous_translation(self):
  source=self.source();notice=self.notice();store,row=self.event(source,notice)
  changed=self.notice('Unknown',raw_sha256='changed');upsert_notice(store,source,changed,NOW);updated=store.get('gameEvents/'+row['id'])
  self.assertEqual(updated['translations'],{});self.assertEqual(updated['originalTitle'],changed['original_title'])
 def test_weapon_templates_require_verified_weapon_dictionary_and_body_role(self):
  notice=self.notice();notice.update(original_title='Military Upgrade | Synthetic Weapon',original_description='The drop rate for the Elite Weapon [Synthetic Weapon] is increased')
  _,row=self.event(notice=notice);self.assertEqual(row['translations']['ko']['title'],'검증 무기 · 무기 픽업')
  notice['original_description']='The drop rate for the Elite Doll [Synthetic Weapon] is increased';_,row=self.event(notice=notice);self.assertEqual(row['translations'],{})
 def test_official_alphanumeric_weapon_model_name_is_preserved_as_a_name(self):
  source=self.source();source['content_localization']['terms'].append({'kind':'weapon','sourceName':'6P33','names':{'ko':'6P33'},'checkedAt':NOW.isoformat(),'evidenceUrl':'https://official.example/model'})
  notice=self.notice();notice.update(original_title='Military Upgrade | 6P33',original_description='The drop rate for the Elite Weapon [6P33] is increased')
  _,row=self.event(source,notice);self.assertEqual(row['translations']['ko']['title'],'6P33 · 무기 픽업')
 def test_official_unchanged_character_model_name_is_preserved(self):
  source=self.source();source['content_localization']['terms'].append({'kind':'character','sourceName':'OTs-14','names':{'ko':'OTs-14'},'checkedAt':NOW.isoformat(),'evidenceUrl':'https://official.example/roster'})
  _,row=self.event(source,self.notice('OTs-14'));self.assertEqual(row['translations']['ko']['title'],'OTs-14 · 캐릭터 픽업')
 def test_known_event_title_generates_no_rewards_or_uncertainty_description(self):
  notice=self.notice();notice.update(original_title='Synthetic Festival',event_type_candidates=['limited-event'],_section_category='limited-event',pickups=[])
  _,row=self.event(notice=notice);self.assertEqual(row['translations']['ko']['title'],'검증 축제');self.assertEqual(row['translations']['ko']['description'],'')
 def test_policy_fields_survive_firestore_roundtrip_and_reseed(self):
  source=self.source(original_image_reference={'mode':'matched_original_link_v1','reviewedAt':NOW.isoformat(),'evidenceUrl':'https://official.example/image-link','allowedHosts':['cdn.example']})
  row=source_row(source,SOURCE['game_id']);self.assertEqual(operating_source(row)['content_localization'],source['content_localization']);self.assertEqual(operating_source(row)['original_image_reference'],source['original_image_reference'])
  row['enabled']=True;store=MemoryStore({'gameSources/source':row});seed_catalogue(store,[{'id':SOURCE['game_id']}],{'games':[{'gameId':SOURCE['game_id'],'sources':[SOURCE]}]})
  self.assertEqual(store.get('gameSources/source')['contentLocalization'],source['content_localization'])
 def image_source(self,**changes):
  return self.source(original_image_reference={'mode':'matched_original_link_v1','reviewedAt':NOW.isoformat(),'evidenceUrl':'https://official.example/linked-images','allowedHosts':['cdn.example']},**changes)
 def test_unique_actual_labelled_publisher_image_gets_an_original_link_without_reuse_rights(self):
  notice=self.notice();notice['original_description']+='<img src="https://cdn.example/current.webp" alt="Targeted Procurement Event | Mityl pickup">'
  _,row=self.event(self.image_source(),notice);self.assertEqual(row['image']['url'],'https://cdn.example/current.webp');self.assertEqual(row['image']['rightsStatus'],'UNCONFIRMED');self.assertEqual(row['image']['sourceReference']['allowedImageUrl'],row['image']['url'])
 def test_unlabelled_wrong_character_or_multiple_images_never_become_fallback(self):
  for markup in ('<img src="https://cdn.example/wrong.webp" alt="Different pickup">','<img src="https://cdn.example/unlabelled.webp">','<img src="https://cdn.example/a.webp" alt="Targeted Procurement Event | Mityl pickup"><img src="https://cdn.example/b.webp" alt="Targeted Procurement Event | Mityl pickup">'):
   _,row=self.event(self.image_source(),self.notice(original_description=self.notice()['original_description']+markup));self.assertIsNone(row['image'])
 def test_changed_notice_cannot_keep_old_automatic_image(self):
  source=self.image_source();notice=self.notice();notice['original_description']+='<img src="https://cdn.example/current.webp" alt="Targeted Procurement Event | Mityl pickup">';store,row=self.event(source,notice)
  upsert_notice(store,source,self.notice(raw_sha256='changed-source'),NOW);self.assertIsNone(store.get('gameEvents/'+row['id'])['image'])
 def test_original_image_reference_optin_is_required(self):
  notice=self.notice();notice['original_description']+='<img src="https://cdn.example/current.webp" alt="Targeted Procurement Event | Mityl pickup">';_,row=self.event(self.source(),notice);self.assertIsNone(row['image'])
 def reviewed_reference(self,notice):
  return {'sourceSha256':notice['raw_sha256'],'sourceTitle':notice.get('_section_title')or notice['original_title'],'officialUrl':notice['official_url'],
   'url':'https://cdn.example/reviewed-current.webp','sourceUrl':'https://official.example/news','kind':'PICKUP_ART',
   'characterNames':notice.get('_raw_character_names',[]),'evidenceText':'Exact reviewed current publisher poster','checkedAt':NOW.isoformat()}
 def test_unlabelled_exact_reviewed_banner_policy_reapplies_only_to_bound_source(self):
  notice=self.notice(_raw_character_names=['Mityl']);ref=self.reviewed_reference(notice);source=self.source(reviewed_notice_references={'mode':'exact_reviewed_notice_links_v1','references':[ref]})
  store,row=self.event(source,notice);self.assertEqual(row['image']['url'],ref['url']);self.assertEqual(row['image']['rightsStatus'],'UNCONFIRMED')
  changed={**notice,'raw_sha256':'changed'};upsert_notice(store,source,changed,NOW);self.assertIsNone(store.get('gameEvents/'+row['id'])['image'])
 def test_exact_reference_title_and_subject_role_cannot_be_substituted(self):
  notice=self.notice(_raw_character_names=['Mityl'])
  for change in ({'sourceTitle':'Another pickup'},{'kind':'EVENT_ART'},{'characterNames':['Different']},{'officialUrl':'https://official.example/other'}):
   ref={**self.reviewed_reference(notice),**change};_,row=self.event(self.source(reviewed_notice_references={'mode':'exact_reviewed_notice_links_v1','references':[ref]}),notice);self.assertIsNone(row['image'])
 def test_competing_exact_reviewed_banner_references_do_not_pick_the_first(self):
  notice=self.notice();ref=self.reviewed_reference(notice);_,row=self.event(self.source(reviewed_notice_references={'mode':'exact_reviewed_notice_links_v1','references':[ref,{**ref,'url':'https://cdn.example/second.webp'}]}),notice);self.assertIsNone(row['image'])
 def test_reviewed_main_calendar_role_does_not_change_schedule_or_notification_approval(self):
  notice=self.notice(_raw_character_names=['Mityl']);role={'sourceSha256':notice['raw_sha256'],'sourceTitle':notice['original_title'],'characterNames':['Mityl'],
   'role':'main-character','evidenceUrl':'https://official.example/main-banner','checkedAt':NOW.isoformat()}
  source=self.source(calendar_role_policy={'mode':'reviewed_main_roles_v1','roles':[role]});store,row=self.event(source,notice)
  self.assertEqual(row['calendarRole'],'main-character');self.assertEqual(row['verificationStatus'],'pending');self.assertIsNone(row['startsAt']);self.assertEqual(row['serverRegion'],'UNSPECIFIED')
  self.assertEqual(store.list('pickupSchedules')[0][1]['calendarRole'],'main-character')
  upsert_notice(store,source,{**notice,'raw_sha256':'changed'},NOW);self.assertIsNone(store.get('gameEvents/'+row['id'])['calendarRole'])
 def test_wrong_calendar_role_character_or_review_never_creates_main_calendar_item(self):
  notice=self.notice(_raw_character_names=['Mityl'])
  for patch in ({'sourceSha256':'wrong'},{'characterNames':['Other']},{'role':'main-event'},{'checkedAt':'unconfirmed'}):
   role={'sourceSha256':notice['raw_sha256'],'sourceTitle':notice['original_title'],'characterNames':['Mityl'],'role':'main-character','evidenceUrl':'https://official.example/main','checkedAt':NOW.isoformat(),**patch}
   _,row=self.event(self.source(calendar_role_policy={'mode':'reviewed_main_roles_v1','roles':[role]}),notice);self.assertIsNone(row['calendarRole'])

class OfficialPairedRosterRefreshTests(unittest.TestCase):
 def source(self):
  source=AutomaticReviewedLocalizationTests().source();source['game_id']='girls-frontline-2';source['content_localization']['gameId']='girls-frontline-2'
  source['localization_roster']={'mode':'gf2_paired_roster_v1','enabled':True,'gameId':'girls-frontline-2','sourceLanguage':'en',
   'sourceUrl':GF2_ROSTER_URLS['en'],'targetUrls':{'ko':GF2_ROSTER_URLS['ko']},'accessStatus':'approved','termsUrl':'https://www.haoplay.com/services/?l=kr#terms',
   'termsCheckedAt':NOW.isoformat(),'identityCheckedAt':NOW.isoformat()}
  return source
 def html(self,rows):
  return ('<!doctype html><html><body>'+''.join('<a class="allroles-roles-main-div kv-change-all-div" href="./?role='+identity+'" target="_blank"><img src="https://portrait.example/1077.png"><div class="allroles-roles-name-name-div">'+name+'<div class="allroles-roles-name-icon-div"><img src="https://icon.example/ignore.png"></div></div></a>' for identity,name in rows)+'</body></html>').encode()
 def fetcher(self,requests):
  payloads={GF2_ROSTER_URLS['en']:self.html([('58','Mityl'),('53','Cheyanne')]),GF2_ROSTER_URLS['ko']:self.html([('53','샤이엔'),('58','미틸')])}
  def fetch(request):requests.append(request);return 200,payloads[request['url']],{}
  return fetch
 def store(self,source):return MemoryStore({'gameSources/source':source_row(source,'girls-frontline-2')})
 def test_names_are_paired_by_stable_role_id_without_reading_portrait_urls(self):
  self.assertEqual(parse_official_roster(self.html([('58','미틸'),('53','샤이엔')]),GF2_ROSTER_URLS['ko']),{'58':'미틸','53':'샤이엔'})
 def test_two_reviewed_pages_update_the_official_dictionary_and_no_image_is_fetched(self):
  source=self.source();store=self.store(source);requests=[];updated,result=refresh_localization_roster(store,source,NOW,self.fetcher(requests),lambda _:None)
  self.assertEqual(result['status'],'SUCCESS');self.assertEqual(result['dictionaryWrites'],1);self.assertEqual(result['pairedNames'],2);self.assertEqual(len(requests),2)
  self.assertEqual({request['url'] for request in requests},set(GF2_ROSTER_URLS.values()));self.assertTrue(all(request['max_response_bytes']==1048576 and request['request_timeout_seconds']==10 for request in requests))
  terms=updated['content_localization']['terms'];self.assertEqual(next(t for t in terms if t['sourceName']=='Cheyanne')['names']['ko'],'샤이엔');self.assertEqual(next(t for t in terms if t['sourceName']=='Mityl')['publisherUnitId'],'58')
  self.assertEqual(updated['content_localization']['version'],2)
 def test_identical_roster_recheck_does_not_write_or_change_dictionary_version(self):
  source=self.source();store=self.store(source);updated,_=refresh_localization_roster(store,source,NOW,self.fetcher([]),lambda _:None);before=store.writes
  again,result=refresh_localization_roster(store,updated,NOW+timedelta(minutes=15),self.fetcher([]),lambda _:None)
  self.assertEqual(result['status'],'SUCCESS');self.assertEqual(result['dictionaryWrites'],0);self.assertEqual(store.writes,before);self.assertEqual(again,updated)
 def test_unmatched_role_id_does_not_get_a_guessed_target_name(self):
  source=self.source();store=self.store(source)
  def fetch(request):return 200,self.html([('58','Mityl'),('99','Unknown')] if request['url']==GF2_ROSTER_URLS['en'] else [('58','미틸')]),{}
  updated,result=refresh_localization_roster(store,source,NOW,fetch,lambda _:None)
  self.assertEqual(result['unmatchedSourceUnits'],1);self.assertFalse(any(t['sourceName']=='Unknown' for t in updated['content_localization']['terms']))
 def test_failed_roster_keeps_verified_dictionary_and_creates_no_dictionary_writes(self):
  source=self.source();store=self.store(source);updated,result=refresh_localization_roster(store,source,NOW,lambda _:(503,b'',{}),lambda _:None)
  self.assertEqual(result['status'],'FETCH_FAILED');self.assertEqual(updated,source);self.assertEqual(store.writes,0)
 def test_unreviewed_or_arbitrary_roster_url_never_executes_a_request(self):
  for patch in ({'accessStatus':'unknown'},{'termsCheckedAt':None},{'sourceUrl':'https://unrelated.example/allroles.html'},{'targetUrls':{'ko':'https://gf2.haoplay.com/kr/other.html'}}):
   source=self.source();source['localization_roster'].update(patch);requests=[];updated,result=refresh_localization_roster(self.store(source),source,NOW,self.fetcher(requests),lambda _:None)
   self.assertEqual(result['status'],'REVIEW_REQUIRED');self.assertEqual(requests,[]);self.assertEqual(updated,source)
 def test_admin_policy_change_is_preserved_instead_of_overwritten(self):
  source=self.source();store=self.store(source);store.records['gameSources/source']['contentLocalization']['version']=99
  _,result=refresh_localization_roster(store,source,NOW,self.fetcher([]),lambda _:None)
  self.assertEqual(result['status'],'SOURCE_POLICY_CHANGED');self.assertEqual(store.writes,0);self.assertEqual(store.get('gameSources/source')['contentLocalization']['version'],99)
 def test_conflicting_or_missing_role_names_are_rejected(self):
  for raw in (self.html([('58','Mityl'),('58','Different')]),b'<html><body>No named roster</body></html>',b'x'*1048577):
   with self.assertRaises(ValidationError):parse_official_roster(raw,GF2_ROSTER_URLS['en'])
 def test_roster_failure_isolated_from_successful_official_notice_collection(self):
  source=self.source();store=self.store(source);requests=[]
  def fetch(request):
   requests.append(request['url'])
   if request['url']in GF2_ROSTER_URLS.values():return 503,b'',{}
   return 200,response(),{}
  result=collect_one(store,source,NOW-timedelta(hours=72),NOW,fetch=fetch,sleep=lambda _:None,now=NOW)
  self.assertEqual(result['status'],'success');self.assertEqual(result['metrics']['localizationRoster']['status'],'FETCH_FAILED');self.assertEqual(len(store.list('collectionErrors')),1);self.assertEqual(len(store.list('gameEvents')),1)
 def test_active_source_lease_prevents_roster_and_notice_requests(self):
  source=self.source();store=self.store(source);store.records['collectorLocks/source']={'leaseUntil':NOW+timedelta(minutes=5)};requests=[]
  result=collect_one(store,source,NOW-timedelta(hours=72),NOW,fetch=self.fetcher(requests),sleep=lambda _:None,now=NOW)
  self.assertEqual(result['status'],'DEFERRED');self.assertEqual(requests,[])

class AdminEnrichmentPolicyTests(unittest.TestCase):
 def test_admin_can_save_reviewed_localization_roster_and_original_link_metadata(self):
  source=OfficialPairedRosterRefreshTests().source();source['original_image_reference']={'mode':'matched_original_link_v1','reviewedAt':NOW,'evidenceUrl':'https://official.example/links','allowedHosts':['cdn.example']}
  source['content_localization']['reviewedAt']=NOW
  source['content_localization']['terms'][0]['checkedAt']=NOW
  result=validate_change('gameSources','source',source_row(source,'girls-frontline-2'))
  self.assertEqual(result['localizationRoster']['targetUrls'],{'ko':GF2_ROSTER_URLS['ko']});self.assertEqual(result['contentLocalization']['terms'][0]['names']['ko'],'미틸')
 def test_admin_rejects_unknown_or_wrong_game_localization_policy(self):
  for change in ({'gameId':'other'},{'terms':[]},{'formats':['arbitrary_regex']},{'privateExtra':'value'}):
   source=OfficialPairedRosterRefreshTests().source();source['content_localization'].update(change)
   with self.assertRaises(SafetyError):validate_change('gameSources','source',source_row(source,'girls-frontline-2'))
 def test_admin_rejects_private_secret_fields_in_nested_name_policy(self):
  source=OfficialPairedRosterRefreshTests().source();source['content_localization']['terms'][0]['privateKey']='not-a-real-key'
  with self.assertRaisesRegex(SafetyError,'secret_field_forbidden'):validate_change('gameSources','source',source_row(source,'girls-frontline-2'))
 def test_admin_original_links_cannot_claim_reuse_rights_or_arbitrary_hosts(self):
  for policy in ({'mode':'matched_original_link_v1','reviewedAt':NOW,'evidenceUrl':'https://official.example/links','allowedHosts':['localhost']},
   {'mode':'matched_original_link_v1','reviewedAt':NOW,'evidenceUrl':'https://official.example/links','allowedHosts':['cdn.example'],'rightsStatus':'APPROVED'}):
   source=OfficialPairedRosterRefreshTests().source();source['original_image_reference']=policy
   with self.assertRaises(SafetyError):validate_change('gameSources','source',source_row(source,'girls-frontline-2'))
 def test_admin_rejects_unbound_reviewed_banner_and_calendar_role_snapshots(self):
  for field,value in [('reviewed_notice_references',{'mode':'exact_reviewed_notice_links_v1','references':[{'sourceSha256':'invalid'}]}),
   ('calendar_role_policy',{'mode':'reviewed_main_roles_v1','roles':[{'sourceSha256':'a'*64,'sourceTitle':'Official','role':'guessed','evidenceUrl':'https://official.example/role','checkedAt':NOW}]})]:
   source=OfficialPairedRosterRefreshTests().source();source[field]=value
   with self.assertRaises(SafetyError):validate_change('gameSources','source',source_row(source,'girls-frontline-2'))
 def test_admin_rejects_unreviewed_or_replaced_roster_endpoint(self):
  source=OfficialPairedRosterRefreshTests().source();source['localization_roster']['targetUrls']['ko']='https://other.example/allroles.html'
  with self.assertRaisesRegex(SafetyError,'exact_paired_roster_policy_required'):validate_change('gameSources','source',source_row(source,'girls-frontline-2'))

class OfficialAnnouncementNotificationTests(unittest.TestCase):
 """Synthetic official-source/user fixtures; zero provider/network requests."""
 GAME='girls-frontline-2'
 def source(self,region='KR'):
  return {**SOURCE,'game_id':self.GAME,'server_region':region,'enabled':True,'section_schedule_format':'heading_period_v1',
   'highest_character_rarity':{'labels':['Elite'],'evidenceUrl':'https://steamcommunity.com/announcement/tiers','checkedAt':NOW.isoformat()},
   'calendar_role_policy':{'mode':'reviewed_main_roles_v1','roles':[],'automaticRules':{'mode':'explicit_main_role_extraction_v1','gameId':self.GAME,'parser':'steam_news','newHighestCharacter':True,
    'mainEventFormat':'gf2_story_event_v1','evidenceUrl':'https://steamcommunity.com/announcement/format','checkedAt':NOW.isoformat()}}}
 def fixture(self,region='KR',global_opt_in=False,arm=True):
  source=self.source(region);store=MemoryStore({
   'users/alice':{'uid':'alice','language':'ko','regions':['KR'],'timezone':'UTC','notificationsEnabled':True},
   'users/alice/userGames/'+self.GAME:{'uid':'alice','gameId':self.GAME,'enabled':True,'createdAt':NOW-timedelta(days=1)},
   'users/alice/notificationSettings/global':{'uid':'alice','enabled':True,'types':['event'],'eventEnabled':True,'announcementEnabled':True,
    'globalAnnouncementsEnabled':global_opt_in,'announcementSubscribedAt':NOW-timedelta(days=1),'regions':['KR'],'eventCategories':['all'],'stages':[],'offsets':[],'timezone':'UTC'},
   'users/alice/notificationSubscriptions/device':{'uid':'alice','transport':'fcm','token':'synthetic-device','platform':'android','enabled':True},
   'gameSources/source':{**source_row(source,self.GAME),'enabled':True}})
  if arm:self.assertEqual(plan_announcement_user(store,'alice',[],NOW),0)
  return store,source
 def publish(self,store,source,clock=None,text=None,key='new-article',updated=None):
  clock=clock or NOW+timedelta(minutes=1)
  body=text or '■Targeted Procurement Event | Fresh\nNew Elite Doll [Fresh] debuts in the Targeted Procurement!'
  raw={'title':'Official new release','contents':body,'date':clock.isoformat(),'updated':updated.isoformat() if updated else None}
  from collector import digest
  notice={**document(region=source['server_region'],sha=digest(raw)),'external_key':key,'original_title':raw['title'],'original_description':body,
   'raw_json':raw,'published_at':clock.isoformat(),'source_updated_at':updated.isoformat() if updated else None,'pickups':[]}
  upsert_notice(store,source,notice,updated or clock)
  return [row for _,row in store.list('gameEvents') if row.get('calendarRole') in ['main-character','main-event']]
 def job(self,store,rows,now=None):
  now=now or NOW+timedelta(minutes=2);self.assertEqual(plan_announcement_user(store,'alice',rows,now),1)
  jobs=store.list('users/alice/notificationJobs');self.assertEqual(len(jobs),1);return jobs[0][0]
 def test_source_specific_new_highest_character_gets_bound_role_without_dates(self):
  s,source=self.fixture();rows=self.publish(s,source);self.assertEqual(len(rows),1);e=rows[0]
  self.assertEqual(e['calendarRole'],'main-character');self.assertEqual(e['calendarRoleEvidence']['sourceSha256'],e['rawSha256'])
  self.assertEqual(e['verificationStatus'],'pending');self.assertIsNone(e['startsAt']);self.assertIsNone(e['endsAt'])
  self.assertTrue(eligible(s,'alice',e,ANNOUNCEMENT_KIND,NOW+timedelta(minutes=2)));self.assertFalse(eligible(s,'alice',e,'event:start',NOW))
 def test_new_named_story_event_uses_exact_heading_and_own_game_modes(self):
  s,source=self.fixture();rows=self.publish(s,source,text='■[Actual Story]\nDuring the Time-Limited Event [Actual Story], Commanders can receive rewards by participating in event Game Modes.')
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['calendarRole'],'main-event');self.job(s,rows)
 def test_side_rewards_login_shop_and_unnamed_events_never_get_main_role(self):
  for text in ['■[Supply Rewards]\nDuring the event, log in to claim rewards.','■[Shop]\nDuring the Time-Limited Event [Other], earn rewards by participating in event Game Modes.',
   '■[Actual Story]\nDuring the event, clear stages.','During the Time-Limited Event [Actual Story], earn rewards by participating in event Game Modes.']:
   s,source=self.fixture();self.assertEqual(self.publish(s,source,text=text),[])
 def test_rerun_weapon_standard_and_ambiguous_targets_never_notify(self):
  for line in ['Returning Elite Doll [Fresh] joins the Targeted Procurement!','New Elite Weapon [Fresh] debuts in the Targeted Procurement!',
   'New Standard Doll [Fresh] debuts in the Targeted Procurement!','New and returning Elite Doll [Fresh] appears!','New Elite Doll [Fresh] and [Other] debut!']:
   s,source=self.fixture();self.assertEqual(self.publish(s,source,text='■Targeted Procurement Event | Fresh\n'+line),[])
 def test_existing_procurement_heading_does_not_infer_new_character(self):
  s,source=self.fixture();self.assertEqual(self.publish(s,source,text='■Targeted Procurement Event | Fresh\nThe drop rate for the Elite Doll [Fresh] is increased!'),[])
 def test_unknown_highest_tier_never_enables_automatic_main_character(self):
  s,source=self.fixture();source.pop('highest_character_rarity');self.assertEqual(self.publish(s,source),[])
 def test_automatic_main_event_format_is_not_reused_for_other_games(self):
  s,source=self.fixture();source['game_id']='other-game';source['calendar_role_policy']['automaticRules']['gameId']='other-game'
  rows=self.publish(s,source,text='■[Actual Story]\nDuring the Time-Limited Event [Actual Story], earn rewards by participating in event Game Modes.');self.assertEqual(rows,[])
 def test_policy_requires_explicit_source_game_parser_and_review(self):
  for field,value in [('gameId','other'),('parser','other'),('checkedAt','yesterday'),('mode','guess'),('evidenceUrl','http://localhost/')]:
   s,source=self.fixture();source['calendar_role_policy']['automaticRules'][field]=value;self.assertEqual(self.publish(s,source),[])
 def test_first_connection_never_replays_recent_72_hour_backfill(self):
  s,source=self.fixture(arm=False);rows=self.publish(s,source,clock=NOW-timedelta(hours=1));self.assertEqual(plan_announcement_user(s,'alice',rows,NOW),0)
  self.assertEqual(s.list('users/alice/notificationJobs'),[]);self.assertEqual(s.get('users/alice/announcementNotificationState/'+self.GAME)['baselineAt'],NOW)
 def test_regional_publication_notifies_even_without_schedule_stages(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);job=s.get('users/alice/notificationJobs/'+key)
  self.assertEqual(job['kind'],ANNOUNCEMENT_KIND);self.assertNotIn('eventRevision',job);self.assertEqual(job['scope'],'regional_official_notice')
 def test_unknown_server_requires_separate_explicit_global_notice_opt_in(self):
  s,source=self.fixture(region='UNSPECIFIED');rows=self.publish(s,source);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
  s.records['users/alice/notificationSettings/global']['globalAnnouncementsEnabled']=True;self.job(s,rows)
  self.assertEqual(rows[0]['serverRegion'],'UNSPECIFIED');self.assertIsNone(rows[0]['startsAt'])
 def test_unknown_scope_is_announced_as_unconfirmed_and_routed_to_real_event(self):
  s,source=self.fixture(region='UNSPECIFIED',global_opt_in=True);rows=self.publish(s,source);key=self.job(s,rows)
  payload=fixed_payload(s,'alice',{**s.get('users/alice/notificationJobs/'+key),'id':key},rows[0],NOW+timedelta(minutes=2))
  self.assertEqual(payload['scope'],'unscoped_official_notice');self.assertIn('서버 지역이 확인되지 않은',payload['body']);self.assertEqual(payload['url'],'/events/'+rows[0]['id'])
  self.assertEqual(payload['announcementRole'],'main-character');self.assertNotIn('Fresh',payload['title'])
 def test_known_other_server_is_not_a_global_unscoped_notice(self):
  s,source=self.fixture(region='JP',global_opt_in=True);rows=self.publish(s,source);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_explicit_global_region_requires_global_region_interest(self):
  s,source=self.fixture(region='GLOBAL',global_opt_in=True);rows=self.publish(s,source);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
  s.records['users/alice']['regions']=['GLOBAL'];s.records['users/alice/notificationSettings/global']['regions']=['GLOBAL'];self.job(s,rows)
 def test_global_game_and_event_type_opt_outs_block_announcement_jobs(self):
  for collection,field,value in [('users/alice','notificationsEnabled',False),('users/alice/notificationSettings/global','enabled',False),
   ('users/alice/notificationSettings/global','announcementEnabled',False),('users/alice/notificationSettings/global','eventEnabled',False),
   ('users/alice/notificationSettings/global','types',['reservation']),('users/alice/userGames/'+self.GAME,'enabled',False)]:
   s,source=self.fixture();rows=self.publish(s,source);s.records[collection][field]=value;self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_game_specific_opt_out_category_and_region_filters_are_rechecked(self):
  for prefs in [{'enabled':False},{'announcementEnabled':False},{'eventEnabled':False},{'types':['reservation']},{'eventCategories':['update']},{'regions':['JP']}]:
   s,source=self.fixture();rows=self.publish(s,source);s.put('users/alice/notificationSettings/'+self.GAME,{'uid':'alice',**prefs})
   self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_profile_region_change_overrides_stale_notification_regions(self):
  s,source=self.fixture();rows=self.publish(s,source);s.records['users/alice']['regions']=['JP'];self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_global_category_filter_cannot_be_overridden_by_game_all(self):
  s,source=self.fixture();rows=self.publish(s,source);s.records['users/alice/notificationSettings/global']['eventCategories']=['update'];s.put('users/alice/notificationSettings/'+self.GAME,{'eventCategories':['all']})
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_quiet_hours_block_publish_jobs_but_do_not_move_baseline(self):
  s,source=self.fixture();rows=self.publish(s,source);s.records['users/alice/notificationSettings/global'].update(quietStart='09:00',quietEnd='11:00')
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0);self.assertEqual(s.get('users/alice/announcementNotificationState/'+self.GAME)['baselineAt'],NOW)
  s.records['users/alice/notificationSettings/global'].update(quietStart=None,quietEnd=None);self.job(s,rows)
 def test_reenable_baseline_discards_off_period_notices(self):
  s,source=self.fixture();s.records['users/alice/notificationSettings/global']['announcementEnabled']=False;plan_announcement_user(s,'alice',[],NOW+timedelta(minutes=1))
  rows=self.publish(s,source,clock=NOW+timedelta(minutes=2));s.records['users/alice/notificationSettings/global']['announcementEnabled']=True
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=3)),0)
 def test_fresh_subscription_timestamp_invalidates_old_armed_state(self):
  s,source=self.fixture();rows=self.publish(s,source);s.records['users/alice/notificationSettings/global']['announcementSubscribedAt']=NOW+timedelta(minutes=2)
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=3)),0)
 def test_deleted_and_reregistered_game_never_replays_old_publications(self):
  s,source=self.fixture();rows=self.publish(s,source);s.records['users/alice/userGames/'+self.GAME]['createdAt']=NOW+timedelta(minutes=2)
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=3)),0)
 def test_wrong_owner_or_game_registration_and_deleted_account_fail_closed(self):
  for path,field,value in [('users/alice/userGames/'+self.GAME,'uid','bob'),('users/alice/userGames/'+self.GAME,'gameId','other'),
   ('users/alice/notificationSettings/global','uid','bob'),('users/alice','deletionRequestedAt',NOW)]:
   s,source=self.fixture();rows=self.publish(s,source);s.records[path][field]=value;self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_repeated_worker_run_and_parallel_plan_create_one_job(self):
  s,source=self.fixture();rows=self.publish(s,source);self.job(s,rows);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=3)),0);self.assertEqual(len(s.list('users/alice/notificationJobs')),1)
 def test_schedule_revision_or_translation_change_does_not_realert_publication(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);changed={**rows[0],'revision':77,'translations':{}};s.put('gameEvents/'+changed['id'],changed)
  self.assertEqual(plan_announcement_user(s,'alice',[changed],NOW+timedelta(minutes=3)),0)
  self.assertEqual(dispatch(s,'alice',key,lambda *_:'mock/provider',NOW+timedelta(minutes=3)),'provider_accepted')
 def test_changed_source_raw_without_explicit_update_timestamp_is_not_recent(self):
  s,source=self.fixture();self.publish(s,source,clock=NOW-timedelta(hours=2));rows=self.publish(s,source,clock=NOW-timedelta(hours=2),text='■Targeted Procurement Event | Fresh\nNew Elite Doll [Fresh] debuts in the Targeted Procurement! Revised details.')
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_explicit_source_update_creates_one_new_content_revision_job(self):
  s,source=self.fixture();rows=self.publish(s,source);self.job(s,rows)
  rows=self.publish(s,source,text='■Targeted Procurement Event | Fresh\nNew Elite Doll [Fresh] debuts in the Targeted Procurement! Revised details.',updated=NOW+timedelta(minutes=3))
  self.assertEqual(rows[0]['announcementRevision'],2);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=4)),1)
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=5)),0);self.assertEqual(len(s.list('users/alice/notificationJobs')),2)
 def test_publication_needs_exact_live_source_role_raw_and_content_evidence(self):
  for field,value in [('calendarRole','other'),('rawSha256','b'*64),('announcementRevision',True),('announcementContentSha256','wrong'),('officialUrl','https://other.example/post'),('sourceId','missing')]:
   s,source=self.fixture();rows=self.publish(s,source);self.assertIsNone(announcement_identity(s,{**rows[0],field:value},NOW+timedelta(minutes=2)))
 def test_source_permission_revocation_and_disabled_source_block_current_jobs(self):
  for field,value in [('enabled',False),('accessStatus','pending'),('approvedForStaging',False),('termsCheckedAt','not-a-time'),('allowedNoticeHosts',['other.example'])]:
   s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);s.records['gameSources/source'][field]=value;sent=[]
   self.assertEqual(dispatch(s,'alice',key,lambda *_:sent.append(True),NOW+timedelta(minutes=3)),'cancelled');self.assertEqual(sent,[])
 def test_missing_raw_collection_document_never_claims_official_publication(self):
  s,source=self.fixture();rows=self.publish(s,source)
  for key,_ in s.list('collectionDocuments'):s.delete('collectionDocuments/'+key)
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),0)
 def test_changed_role_policy_cancels_before_provider_request(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);s.records['gameSources/source']['calendarRolePolicy']['automaticRules']['newHighestCharacter']=False;sent=[]
  self.assertEqual(dispatch(s,'alice',key,lambda *_:sent.append(True),NOW+timedelta(minutes=3)),'cancelled');self.assertEqual(sent,[])
 def test_game_removal_after_planning_cancels_before_provider_request(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);s.delete('users/alice/userGames/'+self.GAME);sent=[]
  self.assertEqual(dispatch(s,'alice',key,lambda *_:sent.append(True),NOW+timedelta(minutes=3)),'cancelled');self.assertEqual(sent,[])
 def test_updated_source_content_cancels_old_revision_job(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);self.publish(s,source,text='■Targeted Procurement Event | Fresh\nNew Elite Doll [Fresh] debuts in the Targeted Procurement! Revised details.',updated=NOW+timedelta(minutes=3));sent=[]
  self.assertEqual(dispatch(s,'alice',key,lambda *_:sent.append(True),NOW+timedelta(minutes=4)),'cancelled');self.assertEqual(sent,[])
 def test_in_app_only_and_provider_acceptance_do_not_assert_human_receipt(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);s.delete('users/alice/notificationSubscriptions/device')
  self.assertEqual(dispatch(s,'alice',key,lambda *_:self.fail('no provider request'),NOW+timedelta(minutes=3)),'in_app_only')
  self.assertIsNone(s.get('users/alice/notificationJobs/'+key)['humanConfirmedAt']);self.assertEqual(s.get('users/alice/notifications/'+key)['status'],'in_app_only')
 def test_provider_failure_is_quarantined_without_retry_or_tokens_in_logs(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows)
  def failed(*_):raise RuntimeError('must not log synthetic token')
  self.assertEqual(dispatch(s,'alice',key,failed,NOW+timedelta(minutes=3)),'delivery_unconfirmed');self.assertEqual(dispatch(s,'alice',key,failed,NOW+timedelta(minutes=4)),'skipped')
  self.assertNotIn('synthetic-device',json.dumps(s.list('notificationLogs'),default=str))
 def test_second_device_is_withheld_if_game_unsubscribed_after_first_send(self):
  s,source=self.fixture();rows=self.publish(s,source);key=self.job(s,rows);s.put('users/alice/notificationSubscriptions/second',{**s.get('users/alice/notificationSubscriptions/device'),'token':'second-device'});sent=[]
  def sender(token,payload):sent.append(token);s.delete('users/alice/userGames/'+self.GAME);return 'mock/provider'
  self.assertEqual(dispatch(s,'alice',key,sender,NOW+timedelta(minutes=3)),'partially_accepted');self.assertEqual(sent,['synthetic-device']);self.assertEqual(s.get('users/alice/notificationJobs/'+key)['withheldDeviceCount'],1)
 def test_source_bound_multi_game_notice_never_invents_other_game_mapping(self):
  s,source=self.fixture();rows=self.publish(s,source);event={**rows[0],'gameIds':[self.GAME,'other']};s.delete('users/alice/userGames/'+self.GAME);s.put('users/alice/userGames/other',{'uid':'alice','gameId':'other','enabled':True})
  self.assertFalse(eligible(s,'alice',event,ANNOUNCEMENT_KIND,NOW+timedelta(minutes=2)))
 def test_future_missing_or_stale_publication_timestamp_never_creates_jobs(self):
  s,source=self.fixture();rows=self.publish(s,source)
  for changes in [{'publishedAt':None},{'announcementPublishedAt':NOW+timedelta(days=1)},{'announcementPublishedAt':NOW-timedelta(days=5)},{'sourceUpdatedAt':'no-zone'}]:
   self.assertIsNone(announcement_identity(s,{**rows[0],**changes},NOW+timedelta(minutes=2)))
 def test_recent_query_is_bounded_sorted_and_skips_missing_clock(self):
  store=MemoryStore({'gameEvents/old':{'announcementPublishedAt':NOW-timedelta(days=4)},'gameEvents/new':{'announcementPublishedAt':NOW},'gameEvents/missing':{},'gameEvents/later':{'announcementPublishedAt':NOW+timedelta(minutes=1)}})
  self.assertEqual([key for key,_ in store.list_since('gameEvents','announcementPublishedAt',NOW-timedelta(days=1))],['later','new'])
  for args in [('users','announcementPublishedAt',NOW,10),('gameEvents','other',NOW,10),('gameEvents','announcementPublishedAt',NOW,201)]:
   with self.assertRaises(SafetyError):store.list_since(*args)
 def test_worker_collects_recent_publications_separately_from_schedule_jobs(self):
  s,source=self.fixture();self.publish(s,source);sent=[];result=notification_run(s,lambda *_:sent.append(True) or 'mock/provider',NOW+timedelta(minutes=2))
  self.assertEqual(result['announcementPlanned'],1);self.assertEqual(result['planned'],0);self.assertEqual(result['providerAcceptedJobs'],1);self.assertEqual(len(sent),1)
 def test_admin_requires_source_specific_bounded_auto_role_review(self):
  source=self.source();row=source_row(source,self.GAME);row['enabled']=True;self.assertEqual(validate_change('gameSources','source',row)['calendarRolePolicy']['automaticRules']['gameId'],self.GAME)
  for changed in [{'gameId':'other'},{'arbitraryRegex':'.*'},{'checkedAt':'missing'},{'mainEventFormat':'guess'}]:
   candidate=json.loads(json.dumps(row,default=lambda value:value.isoformat()));candidate['calendarRolePolicy']['automaticRules'].update(changed)
   with self.assertRaises(SafetyError):validate_change('gameSources','source',candidate)
 def test_account_erasure_removes_server_only_announcement_baselines(self):
  s,_=self.fixture();s.records['users/alice']['deletionRequestedAt']=NOW;result=cleanup_accounts(s,delete_auth=lambda _:None)
  self.assertEqual(result['accountsDeleted'],1);self.assertEqual(s.list('users/alice/announcementNotificationState'),[])
 def test_fcm_data_preserves_publication_role_and_unscoped_notice_scope(self):
  class Messaging:
   Message=staticmethod(lambda **kw:kw)
   AndroidConfig=staticmethod(lambda **kw:kw)
  s,source=self.fixture(region='UNSPECIFIED',global_opt_in=True);rows=self.publish(s,source);key=self.job(s,rows)
  payload=fixed_payload(s,'alice',{**s.get('users/alice/notificationJobs/'+key),'id':key},rows[0],NOW+timedelta(minutes=2));data=fcm_message(Messaging,'synthetic-device',payload)['data']
  self.assertEqual(data['kind'],ANNOUNCEMENT_KIND);self.assertEqual(data['scope'],'unscoped_official_notice');self.assertEqual(data['announcementRole'],'main-character');self.assertEqual(data['recipientUid'],'alice')
 def test_raw_official_source_url_timestamp_and_subject_evidence_are_required(self):
  for field,value in [('officialUrl','https://other.example/post'),('sourceId','other'),('publishedAt',NOW-timedelta(hours=3)),('rawSha256','different')]:
   s,source=self.fixture();rows=self.publish(s,source)
   for key,_ in s.list('collectionDocuments'):s.records['collectionDocuments/'+key][field]=value
   self.assertIsNone(announcement_identity(s,rows[0],NOW+timedelta(minutes=2)))
 def test_cancelled_event_and_unknown_global_opt_out_cancel_pending_notice(self):
  for cancel in ('cancelled','global-opt-out','quiet'):
   s,source=self.fixture(region='UNSPECIFIED',global_opt_in=True);rows=self.publish(s,source);key=self.job(s,rows)
   if cancel=='cancelled':s.records['gameEvents/'+rows[0]['id']]['status']='cancelled'
   elif cancel=='global-opt-out':s.records['users/alice/notificationSettings/global']['globalAnnouncementsEnabled']=False
   else:s.records['users/alice']['dndStart']='00:00';s.records['users/alice']['dndEnd']='00:00'
   self.assertEqual(dispatch(s,'alice',key,lambda *_:self.fail('blocked provider request'),NOW+timedelta(minutes=3)),'cancelled')
 def test_source_region_cannot_be_reinterpreted_as_user_region(self):
  s,source=self.fixture(region='UNSPECIFIED',global_opt_in=True);rows=self.publish(s,source)
  self.assertIsNone(announcement_identity(s,{**rows[0],'serverRegion':'KR'},NOW+timedelta(minutes=2)))
 def test_role_policy_reextraction_does_not_increase_publication_revision(self):
  s,source=self.fixture();rows=self.publish(s,source);self.job(s,rows);source['calendar_role_policy']['automaticRules']['checkedAt']=(NOW+timedelta(seconds=30)).isoformat()
  s.put('gameSources/source',{**source_row(source,self.GAME),'enabled':True});rows=self.publish(s,source)
  self.assertEqual(rows[0]['announcementRevision'],1);self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=3)),0)
 def test_auto_debut_role_needs_valid_highest_rarity_review_clock_and_https(self):
  for changes in ({'checkedAt':'not-verified'},{'evidenceUrl':'http://official.example/tiers'}):
   s,source=self.fixture();source['highest_character_rarity'].update(changes);self.assertEqual(self.publish(s,source),[])
 def new_block_fixture(self):
  s,source=self.fixture();source['calendar_role_policy']['automaticRules']['newCharacterFormat']='gf2_new_doll_block_v1'
  s.put('gameSources/source',{**source_row(source,self.GAME),'enabled':True});return s,source
 def new_block_text(self,name='Fresh'):
  return ('[h3]- New Doll -[/h3][p]■ Elite Doll \\['+name+'][/p][h3]- New Weapons -[/h3][p]■ Elite Weapon [Synthetic Weapon][/p]'
   '[h3]- New Contents -[/h3][p]■ Rate Up Event [Targeted Procurement] and [Military Upgrade].[/p]'
   '[p]・ During the event, the drop rate for Elite Doll ['+name+'] and Elite Weapon [Synthetic Weapon] will be increased.[/p]')
 def test_approved_new_doll_block_creates_one_debut_pickup_without_dates(self):
  s,source=self.new_block_fixture();rows=self.publish(s,source,text=self.new_block_text());self.assertEqual(len(rows),1);e=rows[0]
  self.assertEqual(e['displayTitle'],'Fresh');self.assertEqual(e['announcementKind'],'new');self.assertEqual(e['characterEvidenceMethod'],'gf2_new_doll_block_v1')
  self.assertEqual(e['calendarRoleEvidence']['evidenceText'],e['characterEvidenceText']);self.assertEqual(e['verificationStatus'],'pending');self.assertIsNone(e['startsAt']);self.job(s,rows)
 def test_new_doll_block_requires_independent_source_format_opt_in(self):
  s,source=self.fixture();self.assertEqual(self.publish(s,source,text=self.new_block_text()),[])
 def test_new_doll_block_requires_exact_highest_tier_not_assumed_ssr_alias(self):
  for highest in [None,{'labels':['SSR'],'checkedAt':NOW,'evidenceUrl':'https://official.example/tier'},{'labels':['Elite'],'checkedAt':'unknown','evidenceUrl':'https://official.example/tier'}]:
   s,source=self.new_block_fixture();source['highest_character_rarity']=highest;self.assertEqual(self.publish(s,source,text=self.new_block_text()),[])
 def test_new_doll_block_does_not_accept_reruns_weapons_standard_or_mixed_items(self):
  for text in [self.new_block_text().replace('- New Doll -','- Returning Doll -'),self.new_block_text().replace('- New Doll -','- New and Returning Dolls -'),
   self.new_block_text().replace('■ Elite Doll \\[Fresh]','■ Elite Weapon [Fresh]'),self.new_block_text().replace('■ Elite Doll \\[Fresh]','■ Standard Doll [Fresh]'),
   self.new_block_text().replace('■ Elite Doll \\[Fresh]','■ Elite Doll [Fresh] and [Other]')]:
   s,source=self.new_block_fixture();self.assertEqual(self.publish(s,source,text=text),[])
 def test_new_doll_heading_never_scans_past_unrelated_next_line(self):
  s,source=self.new_block_fixture();text=self.new_block_text().replace('- New Doll -[/h3]','- New Doll -[/h3][p]Unrelated text[/p]')
  self.assertEqual(self.publish(s,source,text=text),[])
 def test_new_doll_block_requires_exact_named_pickup_not_only_a_new_unit(self):
  s,source=self.new_block_fixture();text=self.new_block_text().replace('drop rate for Elite Doll [Fresh]','drop rate for Elite Doll [Other]')
  self.assertEqual(self.publish(s,source,text=text),[])
 def test_new_doll_without_procurement_does_not_infer_gacha_from_mod_release(self):
  s,source=self.new_block_fixture();text='- New Doll -\n■ Elite Doll [Fresh]\nThis unit is obtainable via MOD.'
  self.assertEqual(self.publish(s,source,text=text),[])
 def test_new_doll_parser_can_corroborate_exact_haoplay_procurement_sentence(self):
  s,source=self.new_block_fixture();text='- New Doll -\n■ Elite Doll [Fresh]\n- New Contents -\n■Targeted Procurement Event | Fresh\nThe drop rate for the Elite Doll [Fresh] is increased in the Targeted Procurement!'
  self.assertEqual(len(self.publish(s,source,text=text)),1)
 def test_two_explicit_new_dolls_get_separate_jobs_and_no_parent_duplicate(self):
  s,source=self.new_block_fixture();text=self.new_block_text().replace('- New Doll -','- New Dolls -').replace('\\[Fresh][/p]','\\[Fresh][/p][p]■ Elite Doll [Other][/p]')
  text+='[p]・ During the event, the drop rate for Elite Doll [Other] and Elite Weapon [Synthetic Weapon] will be increased.[/p]'
  rows=self.publish(s,source,text=text);self.assertEqual({row['displayTitle'] for row in rows},{'Fresh','Other'})
  self.assertEqual(plan_announcement_user(s,'alice',rows,NOW+timedelta(minutes=2)),2);self.assertEqual(len(s.list('users/alice/notificationJobs')),2)
 def test_new_doll_block_does_not_borrow_parent_story_event_dates(self):
  s,source=self.new_block_fixture();source.update(schedule_extraction_approved=True,schedule_extraction_reviewed_at=NOW.isoformat(),schedule_extraction_evidence_url='https://steamcommunity.com/announcement/format')
  text=self.new_block_text()+'[p]Event Duration: 2026-10-12 10:00 - 2026-10-20 18:00 (UTC)[/p]';rows=self.publish(s,source,text=text)
  self.assertIsNone(rows[0]['startsAt']);self.assertIsNone(rows[0]['endsAt']);self.assertIsNone(rows[0]['candidateStartsAt']);self.assertEqual(rows[0]['verificationStatus'],'pending')
 def test_new_doll_format_does_not_reuse_gf2_grammar_for_other_games(self):
  s,source=self.new_block_fixture();source['game_id']='other';source['calendar_role_policy']['automaticRules'].update(gameId='other',mainEventFormat=None)
  self.assertEqual(self.publish(s,source,text=self.new_block_text()),[])
 def test_new_doll_block_handles_publisher_bbcode_without_changing_original(self):
  s,source=self.new_block_fixture();body=self.new_block_text();targets=new_doll_block_targets(plain_notice(body),source)
  self.assertEqual([target['name'] for target in targets],['Fresh']);rows=self.publish(s,source,text=body);self.assertEqual(rows[0]['originalDescription'],body)
 def test_new_doll_pickup_role_does_not_depend_on_generic_pickup_keyword_guess(self):
  s,source=self.new_block_fixture();body=self.new_block_text();clock=NOW+timedelta(minutes=1)
  notice={**document(region='KR',sha='d'*64),'original_description':body,'event_type_candidates':['update'],'published_at':clock.isoformat(),'pickups':[]}
  upsert_notice(s,source,notice,clock);rows=[row for _,row in s.list('gameEvents') if row.get('calendarRole')=='main-character']
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['categoryCandidate'],'recruitment');self.assertTrue(eligible(s,'alice',rows[0],ANNOUNCEMENT_KIND,NOW+timedelta(minutes=2)))
 def test_verified_story_period_never_becomes_new_doll_pickup_schedule(self):
  s,source=self.new_block_fixture();source.update(schedule_extraction_approved=True,schedule_extraction_reviewed_at=NOW.isoformat(),schedule_extraction_evidence_url='https://steamcommunity.com/announcement/format')
  rows=self.publish(s,source,text=self.new_block_text()+'[p]Event Duration: 2026-10-12 10:00 - 2026-10-20 18:00 (UTC)[/p]')
  parent=s.get('gameEvents/'+rows[0]['parentEventId']);self.assertEqual(parent['verificationStatus'],'verified');self.assertIsNotNone(parent['startsAt'])
  pickup=next(row for _,row in s.list('pickupSchedules') if row['eventId']==rows[0]['id'])
  for key in ['startsAt','endsAt','originalTimezone','candidateStartsAt','candidateEndsAt','candidateTimezone','lastVerifiedAt','scheduleOrigin']:self.assertIsNone(pickup.get(key),key)
  self.assertEqual(pickup['verificationStatus'],'pending');self.assertEqual(pickup['status'],'unconfirmed')
 def test_unscoped_story_candidate_period_never_becomes_new_doll_pickup_period(self):
  s,source=self.new_block_fixture();source['server_region']='UNSPECIFIED';source.update(schedule_extraction_approved=True,schedule_extraction_reviewed_at=NOW.isoformat(),schedule_extraction_evidence_url='https://steamcommunity.com/announcement/format')
  rows=self.publish(s,source,text=self.new_block_text()+'[p]Event Duration: 2026-10-12 10:00 - 2026-10-20 18:00 (UTC)[/p]')
  parent=s.get('gameEvents/'+rows[0]['parentEventId']);self.assertIsNotNone(parent['candidateStartsAt']);pickup=next(row for _,row in s.list('pickupSchedules') if row['eventId']==rows[0]['id'])
  for key in ['startsAt','endsAt','originalTimezone','candidateStartsAt','candidateEndsAt','candidateTimezone']:self.assertIsNone(pickup.get(key),key)
  self.assertEqual(pickup['serverRegion'],'UNSPECIFIED');self.assertEqual(pickup['verificationStatus'],'pending')
 def test_inline_new_character_inside_main_story_does_not_copy_story_period(self):
  s,source=self.fixture();source.update(schedule_extraction_approved=True,schedule_extraction_reviewed_at=NOW.isoformat(),schedule_extraction_evidence_url='https://steamcommunity.com/announcement/format')
  clock=NOW+timedelta(minutes=1);body='New Elite Doll [Fresh] debuts in the pickup.\nEvent Duration: 2026-10-12 10:00 - 2026-10-20 18:00 (UTC)'
  notice={**document(region='KR',sha='e'*64),'original_title':'Actual Story','original_description':body,'_section_key':'story','_section_title':'Actual Story','_section_category':'limited-event','published_at':clock.isoformat()}
  upsert_notice(s,source,notice,clock);child=next(row for _,row in s.list('gameEvents') if row.get('announcementKind')=='new')
  self.assertIsNotNone(s.get('gameEvents/'+child['parentEventId'])['startsAt']);self.assertEqual(child['categoryCandidate'],'recruitment');self.assertIsNone(child['startsAt']);self.assertEqual(child['verificationStatus'],'pending')
  pickup=next(row for _,row in s.list('pickupSchedules') if row['eventId']==child['id']);self.assertIsNone(pickup['startsAt']);self.assertIsNone(pickup['candidateStartsAt']);self.assertEqual(pickup['verificationStatus'],'pending')
 def test_new_doll_cancellation_is_preserved_when_story_period_is_removed(self):
  s,source=self.new_block_fixture();rows=self.publish(s,source,text=self.new_block_text()+'[p]Event has been cancelled[/p]');self.assertEqual(rows[0]['status'],'cancelled')
  pickup=next(row for _,row in s.list('pickupSchedules') if row['eventId']==rows[0]['id']);self.assertEqual(pickup['status'],'cancelled');self.assertFalse(eligible(s,'alice',rows[0],ANNOUNCEMENT_KIND,NOW+timedelta(minutes=2)))

if __name__=='__main__':unittest.main()
