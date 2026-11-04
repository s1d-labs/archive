"""Create the archive's B2 bucket and its two keys from the master key, and write /etc/archive/b2-{write,read}.env.
Run as root by ~/setup/113-archive.sh. Idempotent for the bucket; keys are created once (B2 shows a key's secret only
at creation, so if a key already exists but its env file is missing, delete it in the B2 console and re-run).
Reads the master key from /etc/scrape-hub/b2-admin.env. Never prints secrets."""
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request

ADMIN = "/etc/scrape-hub/b2-admin.env"
ETC = "/etc/archive"
BUCKET = os.environ.get("ARCHIVE_BUCKET", "s1d-archive")


def read_env(p):
    out = {}
    for line in open(p):
        m = re.match(r"\s*([A-Z0-9_]+)=(.*)", line)
        if m:
            out[m.group(1)] = m.group(2).strip()
    return out


def authorize(kid, key):
    req = urllib.request.Request("https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
                                 headers={"Authorization": "Basic " + base64.b64encode(f"{kid}:{key}".encode()).decode()})
    return json.load(urllib.request.urlopen(req, timeout=30))


def api(auth, call, body):
    req = urllib.request.Request(auth["apiUrl"] + "/b2api/v2/" + call, data=json.dumps(body).encode(),
                                 headers={"Authorization": auth["authorizationToken"], "Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        sys.exit(f"{call} failed ({e.code}): {json.load(e).get('message','')}")


def write_secret(path, kv, comment):
    os.makedirs(ETC, mode=0o700, exist_ok=True)
    with open(path, "w") as f:
        os.chmod(path, 0o600)
        if comment:
            f.write(f"# {comment}\n")
        for k, v in kv.items():
            f.write(f"{k}={v}\n")


def ensure_key(auth, name, caps, bucket_id):
    for k in api(auth, "b2_list_keys", {"accountId": auth["accountId"], "maxKeyCount": 10000})["keys"]:
        if k["keyName"] == name:
            return k, None
    k = api(auth, "b2_create_key", {"accountId": auth["accountId"], "capabilities": caps, "keyName": name, "bucketId": bucket_id})
    return k, k["applicationKey"]


def main():
    e = read_env(ADMIN)
    if not e.get("B2_ADMIN_KEY_ID"):
        sys.exit("no B2 master key at " + ADMIN)
    auth = authorize(e["B2_ADMIN_KEY_ID"], e["B2_ADMIN_KEY"])
    have = api(auth, "b2_list_buckets", {"accountId": auth["accountId"], "bucketName": BUCKET})["buckets"]
    if have:
        b = have[0]
    else:
        b = api(auth, "b2_create_bucket", {"accountId": auth["accountId"], "bucketName": BUCKET, "bucketType": "allPrivate",
                                           "bucketInfo": {"made-by": "archive", "project": "archive.s1d.me"},
                                           "defaultServerSideEncryption": {"mode": "SSE-B2", "algorithm": "AES256"}})
    endpoint = auth["s3ApiUrl"]
    region = endpoint.split("//", 1)[1].split(".")[1]
    common = {"B2_BUCKET": BUCKET, "AWS_ENDPOINT_URL": endpoint, "AWS_DEFAULT_REGION": region}
    for which, name, caps in (("write", f"archive-write", ["listBuckets", "listFiles", "readFiles", "writeFiles", "deleteFiles"]),
                              ("read", f"archive-read", ["listBuckets", "listFiles", "readFiles"])):
        path = f"{ETC}/b2-{which}.env"
        if os.path.exists(path) and read_env(path).get("AWS_ACCESS_KEY_ID"):
            print(f"{which} key: already saved")
            continue
        k, secret = ensure_key(auth, name, caps, b["bucketId"])
        if not secret:
            sys.exit(f"key {name} exists in B2 but its secret isn't saved here; delete it in the B2 console and re-run")
        write_secret(path, {**common, "AWS_ACCESS_KEY_ID": k["applicationKeyId"], "AWS_SECRET_ACCESS_KEY": secret},
                     f"archive {which} key for bucket {BUCKET} (values not shown)")
        print(f"{which} key: created")
    print(f"bucket {BUCKET} ready at {endpoint}")


if __name__ == "__main__":
    main()
