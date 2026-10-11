"""Explicit reviewed older-notice supplement; dry-run by default, no HTTP fetch.

Reads the already fetched publisher response, validates its feed/appid/host, and
prepares five pending outfit rows. Missing start/region/year is never inferred.
The operational collector never imports arbitrary unseen historical notices.
"""
from datetime import datetime, timezone
import argparse, hashlib, json, re
from pathlib import Path
from collector import reviewed, event_identity, upsert_notice, digest, operating_source
from official_adapter import parse_steam_documents
from admin import validate_change
from store import FirestoreStore, SafetyError


def build_bundle(registry, raw):
 game=next(g for g in registry['games'] if g['gameId']=='infinity-nikki')
 source={**game['sources'][0],'game_id':game['gameId']}
 if not reviewed(source):raise SafetyError('source_review_required')
 observed=datetime.now(timezone.utc)
 documents,_=parse_steam_documents(raw,source,observed.isoformat())
 by_id={d['external_key']:d for d in documents}
 old=by_id['1844115010503800'];latest=by_id['1846018067938074']
 text=re.sub(r'\[[^\]]+\]|<[^>]*>',' ',old['original_description'])
 if 'November 26, 2026, at 12:49 (UTC-7)' not in text:raise SafetyError('explicit_end_year_required')
 definitions=[(old,'A Story Dedicated to Children',['If Some One Loves a Flower','The Little Prince'],'2026-11-26T19:49:00Z'),
  (old,'Gilded Judgment',['Dust-Shrouded Crime','Eternal Golden Dream'],None),
  (latest,'The Unfading Wanderer',['Shadowplay Night'],None),
  (latest,'Till the Night Ends',["Eternal Slumber's Vigil"],None),
  (latest,'Palette of Destiny',['Forever Bond','Thought in Bloom'],None)]
 pickups=[]
 for document,name,outfits,end in definitions:
  if any(value not in document['original_description'] for value in [name,*outfits]):raise SafetyError('official_outfit_names_required')
  event_id=event_identity(document,source)
  row={'id':digest({'event':event_id,'game':source['game_id'],'region':'UNSPECIFIED','server':None,'kind':'outfit','name':name}),
   'eventId':event_id,'gameId':source['game_id'],'serverRegion':'UNSPECIFIED','serverLabel':None,
   'officialUrl':document['official_url'],'verificationStatus':'pending','pickupKind':'outfit',
   'characters':[],'weapons':[],'outfits':outfits,'startsAt':None,'endsAt':end,
   'firstCheckedAt':observed,'lastCheckedAt':observed,'firstConfirmedAt':None,'lastVerifiedAt':None}
  pickups.append({'data':row,'eventName':name,'itemRawSha256':document['raw_sha256'],
   'endYearExplicit':end is not None,'endMonthDayClock':'11-26 12:49 UTC-7' if end else '10-29 12:49 UTC-7'})
 return {'schemaVersion':1,'source':source,'olderNotice':old,'currentNoticeEventId':event_identity(latest,source),
  'pickups':pickups,'recordedAt':observed,'feedResponseRawSha256':hashlib.sha256(raw).hexdigest()}


def validate_bundle(bundle):
 source=bundle['source'];old=bundle['olderNotice'];rows=bundle['pickups']
 if not reviewed(source) or not 1<=len(rows)<=5:raise SafetyError('bounded_reviewed_pending_bundle_required')
 expected_events={event_identity(old,source),bundle['currentNoticeEventId']}
 for pickup in rows:
  data=pickup['data']
  if data.get('gameId')!=source['game_id'] or data.get('eventId') not in expected_events:raise SafetyError('pickup_source_identity_mismatch')
  if data.get('verificationStatus')!='pending' or data.get('startsAt') is not None or data.get('serverRegion')!='UNSPECIFIED':raise SafetyError('unconfirmed_pickup_must_remain_pending')
  if data.get('pickupKind')!='outfit' or data.get('characters') or data.get('weapons'):raise SafetyError('outfits_are_not_characters_or_weapons')
  if data.get('endsAt') is not None and pickup.get('endYearExplicit') is not True:raise SafetyError('explicit_end_year_required')
  validate_change('pickupSchedules',data['id'],data)
 return bundle


def apply_bundle(store,bundle):
 validate_bundle(bundle);source=bundle['source'];old=bundle['olderNotice']
 # Check all prerequisites before the first write. Existing manual rows are
 # never overwritten by a repeated partial review/import.
 if store.remote:
  operating=store.get('gameSources/'+source['id']) or {}
  if operating.get('enabled') is not True or operating.get('gameId')!=source['game_id'] or operating.get('steamAppid')!=source['steam_appid'] or not reviewed(operating_source({**operating,'id':source['id']})):raise SafetyError('operating_reviewed_source_required')
 current=store.get('gameEvents/'+bundle['currentNoticeEventId'])
 if not current or current.get('sourceId')!=source['id'] or source['game_id'] not in current.get('gameIds',[]):raise SafetyError('current_notice_reference_missing')
 counts={'noticesCreated':0,'pickupsCreated':0,'existingPickupsPreserved':0}
 old_id=event_identity(old,source)
 if not store.get('gameEvents/'+old_id):
  upsert_notice(store,source,{**old,'pickups':[]},bundle['recordedAt']);counts['noticesCreated']=1
 for pickup in bundle['pickups']:
  data=pickup['data']
  def create_only(existing):
   if existing:return None,False
   return validate_change('pickupSchedules',data['id'],data),True
  changed=store.mutate('pickupSchedules/'+data['id'],create_only)
  counts['pickupsCreated' if changed else 'existingPickupsPreserved']+=1
 return counts


def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--raw-feed',required=True)
 parser.add_argument('--registry',default='data/official-sources.json')
 parser.add_argument('--apply',action='store_true')
 parser.add_argument('--bundle-output',default='work/infinity-nikki-pending-pickup-import.json')
 parser.add_argument('--report',default='reports/infinity-nikki-pending-pickup-import.json')
 args=parser.parse_args();raw=Path(args.raw_feed).read_bytes()
 if len(raw)>2097152:raise SafetyError('bounded_publisher_response_required')
 bundle=validate_bundle(build_bundle(json.loads(Path(args.registry).read_text(encoding='utf-8-sig')),raw))
 target=Path(args.bundle_output);target.parent.mkdir(parents=True,exist_ok=True)
 target.write_text(json.dumps(bundle,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+'\n',encoding='utf-8')
 result={'recordedAt':datetime.now(timezone.utc),'status':'VALIDATED_DRY_RUN','fixtureResponsesUsed':False,
  'apiRequests':0,'remoteFirestoreWrites':0,'realFcmSends':0,'feedResponseRawSha256':bundle['feedResponseRawSha256'],
  'plannedOlderNotices':1,'plannedPickups':len(bundle['pickups']),'rawProofDocuments':1,
  'unknownStartCount':5,'unknownRegionCount':5,'explicitEndYearCount':1,'unconfirmedEndYearCount':4,
  'pendingPickupsOnly':True,'verifiedPickupSchedules':0,'original72HourRunRecordNotRewritten':True,
  'localRawBundlePublished':False,'pickups':bundle['pickups'],'currentNoticeEventId':bundle['currentNoticeEventId'],
  'olderNotice':{'id':event_identity(bundle['olderNotice'],bundle['source']),'externalKey':bundle['olderNotice']['external_key'],
   'officialUrl':bundle['olderNotice']['official_url'],'rawSha256':bundle['olderNotice']['raw_sha256'],'publishedAt':bundle['olderNotice']['published_at']}}
 if args.apply:
  store=FirestoreStore();result.update(apply_bundle(store,bundle));result.update(status='ACTUAL_PENDING_SUPPLEMENT_APPLIED',remoteFirestoreWrites=store.writes)
  store.finish_usage();result.update(usageAccountingWrites=2,totalFirestoreWrites=store.writes+2)
 output=Path(args.report);output.parent.mkdir(parents=True,exist_ok=True)
 output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+'\n',encoding='utf-8')
 print(json.dumps({key:result[key] for key in ['status','plannedOlderNotices','plannedPickups','explicitEndYearCount','unconfirmedEndYearCount','remoteFirestoreWrites','realFcmSends']}))


if __name__=='__main__':main()
