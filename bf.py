#!/bin/python3
import re
import subprocess, json, argparse
import datetime
def init_pg(cluster=None):
  if not cluster:
      cluster = "ceph"
  pg_json = subprocess.run(['ceph','--cluster',cluster,'pg','dump','--format=json'], stdout=subprocess.PIPE)
  pg_data = json.loads(pg_json.stdout)
  return pg_data['pg_map'] if "pg_map" in pg_data else pg_data # respect old versions

def init_from_file():
    with open("mydump.json", "r") as f:
        pg_data = json.loads(f.read())
        f.close()
    # print(pg_data)
    return pg_data['pg_map']

def pgs_scrub(cluster,use_file):
    if use_file:
        stats = init_from_file()['pg_stats']
    else:
        stats = init_pg(cluster=cluster)['pg_stats']
    scrub_list = []
    deep_scrub_list = []
    for pg in stats:
        pgid = pg['pgid']
        # print(pg.keys())
        sdate = pg["last_scrub_stamp"]
        dsdate = pg["last_scrub_stamp"]
        deep_scrub_list.append([pgid, datetime.datetime.strptime(dsdate, "%Y-%m-%d %H:%M:%S.%f"),pg["state"]])
        scrub_list.append([pgid, datetime.datetime.strptime(sdate, "%Y-%m-%d %H:%M:%S.%f"),pg["state"]])
    print("deepscrub:")
    for p in sorted(deep_scrub_list, key = lambda i: i[1]):
        print("%s\t%s\t%s" % (p[0],p[1],p[2]))
    print("scrub: ")
    for p in sorted(scrub_list, key = lambda i: i[1]):
        print("%s\t%s\t%s" % (p[0],p[1],p[2]))
    
    return()
def pgs_backfill(action,osd,cluster,use_file):
    global_progress_list = []
    if osd: osd = int(osd)
    wait, now, too = [],[],[]
    if use_file:
        stats = init_from_file()['pg_stats']
    else:
        stats = init_pg(cluster=cluster)['pg_stats']
    for pg in stats:
        state = pg['state']
        if 'backfill_wait' in state:
            wait.append(pg)
        if 'backfilling' in state:
            now.append(pg)
        if 'backfill_toofull' in state:
            too.append(pg)
        total_progress = []
    print("Backfilling now:")
    pg = None
    for pg in now:
        if osd: 
            up = set(pg['up'])
            acting = set(pg['acting'])
            where_from = set(acting) - set(up)
            where_to = set(up) - set(acting)
            if (osd in where_from or osd in where_to):
                pg_print(pg=pg)
                continue
            else:
                continue
        else:
                pg_print(pg=pg)
    pg = None
    print("Backfill wait:")
    for pg in wait:
        if osd: 
            up = set(pg['up'])
            acting = set(pg['acting'])
            where_from = set(acting) - set(up)
            where_to = set(up) - set(acting)
            if (osd in where_from or osd in where_to):
                pg_print(pg=pg)
                continue
            else:
                continue
        else:
                pg_print(pg=pg)
    pg = None
    print("Toofull:")
    for pg in too:
        if osd:
            up = set(pg['up'])
            acting = set(pg['acting'])
            where_from = set(acting) - set(up)
            where_to = set(up) - set(acting)
            if (osd in where_from or osd in where_to):
                pg_print(pg=pg)
                continue
            else:
                continue
        else:
                pg_print(pg=pg)
    pg = None


def pg_print(pg=None, action=None):
    state = pg['state']
    pgid = pg['pgid']
    up = set(pg['up'])
    acting = set(pg['acting'])
    where_from = set(acting) - set(up)
    where_to = set(up) - set(acting)
    num_bytes = pg['stat_sum']['num_bytes']
    num_bytes_recovered = pg['stat_sum']['num_bytes_recovered']
    num_bytes_left = float(num_bytes - num_bytes_recovered) / 1024 / 1024 / 1024
    num_gb = float(num_bytes)/1024/1024/1024
    num_objects = pg['stat_sum']['num_objects']
    num_objects_copies = pg['stat_sum']['num_object_copies']
    num_faulty_objects = max([pg['stat_sum']['num_objects_degraded'],pg['stat_sum']['num_objects_misplaced']])
    object_perc = 1 - (float(num_faulty_objects) / float(num_objects_copies) if num_objects_copies > 0 else 0)
    print("pg %s %s from osd %s to osd %s size of %.2f, progress: %.4f" % (pgid, state,where_from, where_to, num_gb, object_perc))
    # return(object_perc)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--osd", help = "set osd")
    parser.add_argument("-s", "--state", help = "state osd")
    parser.add_argument("-c", "--cluster", help = "cluster name")  
    parser.add_argument("-f", "--file", help="use file instead of output (ONLY FOR LOCAL DEBUG RUN)", action='store_true')  
    parser.add_argument("-a", "--action", help="scrub or backfill", default="backfill")
    args = parser.parse_args()
    osd = args.osd
    action = args.state
    cluster = args.cluster
    use_file = args.file
    action = args.action
    print(action)
    if (not action or action == "backfill"):
        print(action,type(action),use_file,type(use_file))
        pgs_backfill(action,osd,cluster,use_file)
    elif action == "scrub":
        pgs_scrub(cluster,use_file)
    else: 
        parser.print_help()
    
