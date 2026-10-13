"""Free external worker for reviewed admin edits and requested account erasure.

No cloud Functions are provisioned. Client admin claims permit a review request,
never a direct official write. All secrets remain in this trusted worker runtime.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta,timezone
import json
import re
import uuid
from urllib.parse import urlsplit
from store import FirestoreStore,MemoryStore,SafetyError,timestamp

FIELDS={
 "games":{"id","slug","names","aliases","developer","publisher","platforms","regions","officialWebsite","officialNoticeUrl","officialSocialUrls","serviceStatus","collectionSupport","dailyReset","weeklyReset","resetTimezone","officialIntegration","metadataStatus","nameProvenance","sourceEvidence","image","resetRules","verifiedResetRuleIds","resetRuleById","enabled","createdAt","updatedAt"},
 "gameSources":{"id","gameId","url","parser","steamAppid","enabled","accessStatus","approvedForStaging","termsUrl","termsCheckedAt","noticeHostsVerifiedAt","allowedNoticeHosts","feedAllowlist","intervalMinutes","serverRegion","serverLabel","health","lastCheckedAt","lastSuccessAt","lastErrorAt","lastReverifiedAt","nextRunAt","scheduleExtractionApproved","scheduleExtractionReviewedAt","scheduleExtractionEvidenceUrl","contentLanguage","highestCharacterRarity","imageReuse","sectionScheduleFormat","pairedRecruitmentPeriodApproved"},
 "gameEvents":{"id","sourceId","externalKey","gameIds","serverRegion","serverLabel","title","description","originalTitle","originalDescription","originalLanguage","officialUrl","publishedAt","sourceUpdatedAt","rawSha256","eventTypeCandidates","firstCheckedAt","lastCheckedAt","lastChangedAt","lastVerifiedAt","verificationStatus","revision","startsAt","endsAt","reservationStartsAt","reservationEndsAt","ticketSalesAt","originalTimezone","category","scheduleStatus","status","venue","imageRightsStatus","translations"},
 "pickupSchedules":{"id","eventId","gameId","serverRegion","serverLabel","officialUrl","verificationStatus","lastCheckedAt","firstCheckedAt","firstConfirmedAt","lastChangedAt","pickupKind","characters","weapons","outfits","startsAt","endsAt","lastVerifiedAt"},
}
TIME_FIELDS={"termsCheckedAt","noticeHostsVerifiedAt","lastCheckedAt","lastSuccessAt","lastErrorAt","lastReverifiedAt","nextRunAt","publishedAt","sourceUpdatedAt","firstCheckedAt","lastChangedAt","lastVerifiedAt","startsAt","endsAt","reservationStartsAt","reservationEndsAt","ticketSalesAt","firstConfirmedAt","createdAt","updatedAt"}


def https(value):
 if not isinstance(value,str):return False
 try:
  parsed=urlsplit(value)
  return parsed.scheme=='https' and bool(parsed.hostname) and not parsed.username and not parsed.password and (parsed.port in [None,443])
 except ValueError:return False


def reject_secret_fields(value):
 if isinstance(value,dict):
  for key,item in value.items():
   if re.search(r'(private.?key|secret|password|access.?token|service.?account|credential|api.?key)',key,re.I):raise SafetyError('secret_field_forbidden')
   reject_secret_fields(item)
 elif isinstance(value,list):
  for item in value:reject_secret_fields(item)


def validate_change(collection,document_id,data,old=None):
 if collection not in FIELDS or not re.fullmatch(r'[a-zA-Z0-9_-]{1,200}',document_id):raise SafetyError('invalid_admin_target')
 if not isinstance(data,dict) or set(data)-FIELDS[collection]:raise SafetyError('unexpected_official_fields')
 reject_secret_fields(data)
 row={**(old or {}),**data,'id':document_id}
 for field in TIME_FIELDS:
  if row.get(field) is not None:row[field]=timestamp(row[field])
 if collection=='games':
  if not https(row.get('officialWebsite')):raise SafetyError('official_website_https_required')
  names=row.get('names',{})
  if not isinstance(names,dict) or set(names)-{'ko','en','ja'} or not any(isinstance(v,str) and v for v in names.values()):raise SafetyError('official_game_name_required')
  ids=[];by_id={}
  for rule in row.get('resetRules',[]):
   if not isinstance(rule,dict) or set(rule)-{'id','verified','timezone','dailyTime','evidenceUrl','region','serverRegion','serverLabel'}:raise SafetyError('invalid_reset_rule_fields')
   if rule.get('verified') is not True or not https(rule.get('evidenceUrl')) or not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d',rule.get('dailyTime','')):raise SafetyError('verified_reset_evidence_required')
   if not isinstance(rule.get('id'),str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}',rule['id']) or rule['id'] in ids:raise SafetyError('unique_reset_rule_slug_required')
   if rule.get('region') and rule.get('serverRegion') and rule['region']!=rule['serverRegion']:raise SafetyError('conflicting_reset_region')
   if rule.get('region',rule.get('serverRegion')) not in [None,'KR','JP','GLOBAL']:raise SafetyError('known_reset_region_required')
   from zoneinfo import ZoneInfo
   ZoneInfo(rule['timezone']);ids.append(rule['id']);by_id[rule['id']]=dict(rule)
  row['verifiedResetRuleIds']=ids
  row['resetRuleById']=by_id # Derived from reviewed rules; callers cannot substitute a different index.
 if collection=='gameSources':
  if row.get('parser')!='steam_news' or not isinstance(row.get('steamAppid'),int) or not https(row.get('url')):raise SafetyError('reviewed_source_adapter_required')
  if row.get('intervalMinutes',0)<15:raise SafetyError('minimum_source_interval_15_minutes')
  if row.get('enabled') is True:
   if row.get('accessStatus')!='approved' or not row.get('termsCheckedAt') or not row.get('noticeHostsVerifiedAt') or not https(row.get('termsUrl')) or row.get('feedAllowlist')!=['steam_community_announcements'] or not row.get('allowedNoticeHosts'):raise SafetyError('source_permission_evidence_required')
  if row.get('scheduleExtractionApproved') is True:
   if not https(row.get('scheduleExtractionEvidenceUrl')) or not row.get('scheduleExtractionReviewedAt') or row.get('serverRegion') not in ['KR','JP','GLOBAL','UNSPECIFIED']:raise SafetyError('schedule_extraction_review_and_region_required')
   row['scheduleExtractionReviewedAt']=timestamp(row['scheduleExtractionReviewedAt'])
  if row.get('contentLanguage') not in [None,'ko','en','ja']:raise SafetyError('supported_source_language_required')
  if row.get('sectionScheduleFormat') not in [None,'heading_period_v1']:raise SafetyError('reviewed_section_format_required')
  if row.get('pairedRecruitmentPeriodApproved') is True and (row.get('sectionScheduleFormat')!='heading_period_v1' or row.get('scheduleExtractionApproved') is not True):raise SafetyError('paired_recruitment_format_review_required')
  rarity=row.get('highestCharacterRarity')
  if rarity is not None:
   if not isinstance(rarity,dict) or set(rarity)-{'labels','evidenceUrl','checkedAt'}:raise SafetyError('invalid_rarity_rule')
   labels=rarity.get('labels')
   if not isinstance(labels,list) or not 1<=len(labels)<=10 or any(not isinstance(label,str) or not 1<=len(label)<=30 for label in labels) or not https(rarity.get('evidenceUrl')) or not rarity.get('checkedAt'):raise SafetyError('official_highest_rarity_evidence_required')
   rarity['checkedAt']=timestamp(rarity['checkedAt'])
  images=row.get('imageReuse')
  if images is not None:
   if not isinstance(images,dict) or set(images)-{'rightsStatus','allowedHosts','evidenceUrl','checkedAt'}:raise SafetyError('invalid_image_reuse_policy')
   if images.get('rightsStatus') not in ['APPROVED','UNCONFIRMED','REJECTED']:raise SafetyError('image_rights_status_required')
   hosts=images.get('allowedHosts',[])
   if not isinstance(hosts,list) or len(hosts)>20 or any(not isinstance(host,str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?',host) for host in hosts):raise SafetyError('exact_image_hosts_required')
   if images.get('rightsStatus')=='APPROVED':
    if not hosts or not https(images.get('evidenceUrl')) or not images.get('checkedAt'):raise SafetyError('image_reuse_evidence_required')
    images['checkedAt']=timestamp(images['checkedAt'])
 if collection in ['gameEvents','pickupSchedules']:
  if not https(row.get('officialUrl')):raise SafetyError('official_notice_https_required')
  if collection=='pickupSchedules':
   for field in ['characters','weapons','outfits']:
    values=row.get(field,[])
    if not isinstance(values,list) or len(values)>100 or any(not isinstance(value,str) or not 1<=len(value)<=300 for value in values):raise SafetyError('invalid_pickup_target_names')
   if row.get('outfits') and row.get('pickupKind')!='outfit':raise SafetyError('outfit_targets_require_outfit_kind')
  if row.get('startsAt') and row.get('endsAt') and row['endsAt']<row['startsAt']:raise SafetyError('invalid_schedule_range')
  if row.get('verificationStatus')=='verified':
   if not row.get('lastVerifiedAt') or row.get('serverRegion') in [None,'','UNSPECIFIED'] or not row.get('startsAt'):raise SafetyError('verified_schedule_and_region_required')
   if collection=='gameEvents' and not row.get('originalTimezone'):raise SafetyError('source_timezone_required')
   if collection=='pickupSchedules' and (not row.get('pickupKind') or not row.get('characters') and not row.get('weapons') and not row.get('outfits') or not row.get('endsAt')):raise SafetyError('verified_pickup_targets_required')
  if collection=='gameEvents':row['revision']=(old or {}).get('revision',0)+1
  row['lastChangedAt']=datetime.now(timezone.utc)
 return row


def apply_requests(store,max_requests=25):
 result={'applied':0,'rejected':0}
 for request_id,request in store.list('adminChangeRequests',max_requests):
  token=uuid.uuid4().hex
  def claim(old):
   if not old:return None,None
   recoverable=old.get('status')=='processing' and old.get('startedAt') and timestamp(old['startedAt'])<datetime.now(timezone.utc)-timedelta(minutes=20)
   if old.get('status')!='requested' and not recoverable:return None,None
   return {**old,'status':'processing','workerToken':token,'startedAt':datetime.now(timezone.utc)},old
  request=store.mutate('adminChangeRequests/'+request_id,claim)
  if not request:continue
  target=request.get('collection');doc_id=request.get('documentId');path=str(target)+'/'+str(doc_id)
  try:
   if target not in FIELDS or not re.fullmatch(r'[a-zA-Z0-9_-]{1,200}',str(doc_id)):raise SafetyError('invalid_admin_target')
   # Recheck administrator privileges at execution, including revoked claims.
   if store.remote:
    from firebase_admin import auth
    account=auth.get_user(request['uid'],app=store.app)
    if account.disabled or not account.custom_claims or account.custom_claims.get('admin') is not True:raise SafetyError('admin_permission_revoked')
   if request.get('operation')=='delete':store.delete(path)
   elif request.get('operation')=='upsert':
    def upsert(old):
     if old and old.get('lastAdminRequestId')==request_id:return None,True
     return {**validate_change(target,doc_id,request.get('data'),old),'lastAdminRequestId':request_id},True
    store.mutate(path,upsert)
   else:raise SafetyError('invalid_admin_operation')
   store.put('adminChangeRequests/'+request_id,{**request,'status':'applied','appliedAt':datetime.now(timezone.utc)});result['applied']+=1
  except Exception as error:
   reason=str(error)[:120] if isinstance(error,SafetyError) else type(error).__name__
   store.put('adminChangeRequests/'+request_id,{**request,'status':'rejected','reasonCode':reason,'reviewedAt':datetime.now(timezone.utc)});result['rejected']+=1
 return result


PRIVATE_COLLECTIONS=['userGames','attendanceDaily','executionRecords','deviceRegistrations','notificationSettings','notificationSubscriptions','notificationReceipts','attendanceReminderLimits','bookmarks','reports','notificationJobs','notifications','pushTestLimits']

def cleanup_execution_retention(store,now=None,max_users=20,max_documents=100):
 """Free worker deletion, not paid Firestore TTL. Cursor progresses fairly.

 A bounded pass is eligible every 24 hours. Large backlogs can extend retention;
 callers must inspect `scanLimitReached` instead of claiming exact TTL expiry.
 Existing attendanceDaily summaries are preserved unless the owner requested
 their deletion through executionRecordsDeletedAt; only then are they scanned.
 """
 if not 1<=max_users<=20 or not 1<=max_documents<=100:raise SafetyError('bounded_retention_limits_required')
 now=now or datetime.now(timezone.utc);lock_path='collectorLocks/execution-retention';lease=uuid.uuid4().hex
 def claim(old):
  old=old or {}
  if old.get('leaseUntil') and timestamp(old['leaseUntil'])>now or old.get('nextRunAt') and timestamp(old['nextRunAt'])>now:return None,None
  return {**old,'leaseToken':lease,'leaseUntil':now+timedelta(minutes=12),'updatedAt':now},old
 prior=store.mutate(lock_path,claim)
 if prior is None:return {'status':'DEFERRED','documentsDeleted':0,'attendanceDailyWrites':0}
 cursor={key:prior.get(key) for key in ['afterUser','activeUser','afterRecord']};cursor['collectionIndex']=prior.get('collectionIndex',0)
 collections=[('executionRecords','startedAt',30),('notificationReceipts','createdAt',30),('attendanceReminderLimits','createdAt',35),('attendanceDaily','lastExecutionAt',None)]
 stats={'status':'SUCCESS','usersVisited':0,'recordsScanned':0,'documentsDeleted':0,'explicitMarkerDeletes':0,'invalidTimestamps':0,'invalidOwnership':0,'scanLimitReached':False,'allUsersScanned':False,'attendanceDailyWrites':0};visited=set();profiles={}
 try:
  while (len(visited)<max_users or cursor['activeUser'] in visited) and stats['recordsScanned']<max_documents:
   if not cursor['activeUser']:
    users=store.list_cursor('users',cursor['afterUser'],1)
    if not users:
     cursor.update(afterUser=None,activeUser=None,afterRecord=None,collectionIndex=0);stats['allUsersScanned']=True;break
    cursor['activeUser']=users[0][0]
    profiles[users[0][0]]=users[0][1]
   uid=cursor['activeUser'];visited.add(uid)
   if uid not in profiles:profiles[uid]=store.get('users/'+uid) or {}
   marker=profiles[uid].get('executionRecordsDeletedAt')
   name,field,days=collections[cursor['collectionIndex']]
   if name=='attendanceDaily' and not marker:
    cursor.update(afterUser=uid,activeUser=None,collectionIndex=0,afterRecord=None);continue
   requested=min(25,max_documents-stats['recordsScanned'])
   records=store.list_cursor(f'users/{uid}/{name}',cursor['afterRecord'],requested)
   for record_id,row in records:
    stats['recordsScanned']+=1
    if row.get('uid')!=uid:
     stats['invalidOwnership']+=1;cursor['afterRecord']=record_id;continue
    try:
     value=timestamp(row.get(field) or row.get('updatedAt')) if name=='attendanceDaily' else timestamp(row.get(field))
     explicit=marker and name in ['executionRecords','attendanceDaily'] and value<=timestamp(marker)
     expired=days is not None and value<now-timedelta(days=days)
     if explicit or expired:
      store.delete(f'users/{uid}/{name}/{record_id}');stats['documentsDeleted']+=1
      if explicit:stats['explicitMarkerDeletes']+=1
      if name=='attendanceDaily':stats['attendanceDailyWrites']+=1
    except (ValueError,TypeError):stats['invalidTimestamps']+=1
    cursor['afterRecord']=record_id
   if len(records)<requested:
    cursor['collectionIndex']+=1;cursor['afterRecord']=None
    if cursor['collectionIndex']==len(collections):cursor.update(afterUser=uid,activeUser=None,collectionIndex=0)
   if stats['recordsScanned']==max_documents:stats['scanLimitReached']=True
  stats['usersVisited']=len(visited)
  if len(visited)>=max_users and not stats['allUsersScanned']:stats['scanLimitReached']=True
 except Exception as error:
  stats.update(status='FAILED_RETRYABLE',reasonCode=type(error).__name__,usersVisited=len(visited))
 def finish(old):
  if not old or old.get('leaseToken')!=lease:return None,False
  return {**old,**cursor,'leaseToken':None,'leaseUntil':None,'lastRunAt':now,'nextRunAt':now+(timedelta(minutes=15) if stats['status']=='FAILED_RETRYABLE' else timedelta(hours=24)),'metrics':stats,'updatedAt':now},True
 store.mutate(lock_path,finish)
 return stats
def cleanup_accounts(store,max_users=20,max_documents=300,delete_auth=None):
 stats={'accountsDeleted':0,'documentsDeleted':0,'pending':0}
 for uid,profile in store.list('users',max_users):
  if not profile.get('deletionRequestedAt'):continue
  if store.remote:
   from firebase_admin import auth
   try:auth.update_user(uid,disabled=True,app=store.app);auth.revoke_refresh_tokens(uid,app=store.app)
   except auth.UserNotFoundError:pass
  remaining=False
  for collection in PRIVATE_COLLECTIONS:
   for doc_id,_ in store.list(f'users/{uid}/{collection}',max_documents):
    if stats['documentsDeleted']>=max_documents:remaining=True;break
    store.delete(f'users/{uid}/{collection}/{doc_id}');stats['documentsDeleted']+=1
   if stats['documentsDeleted']>=max_documents:remaining=True;break
  if remaining:stats['pending']+=1;continue
  if store.remote:
   # Old ID tokens can remain valid until expiry; keep the deletion tombstone for
   # >= 65 minutes, then perform a final sweep before deleting Auth/profile.
   if timestamp(profile['deletionRequestedAt'])>datetime.now(timezone.utc)-timedelta(minutes=65):stats['pending']+=1;continue
   try:auth.delete_user(uid,app=store.app)
   except auth.UserNotFoundError:pass
  elif delete_auth:delete_auth(uid)
  store.delete('users/'+uid);stats['accountsDeleted']+=1
 return stats


def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
 if not args.apply:print(json.dumps({'status':'PLAN_ONLY','remoteWrites':0}));return
 store=FirestoreStore();result={'adminRequests':apply_requests(store),'accountCleanup':cleanup_accounts(store),'executionRetention':cleanup_execution_retention(store),'remoteWrites':store.writes};store.finish_usage();print(json.dumps(result))

if __name__=='__main__':main()
