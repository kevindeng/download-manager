import subprocess
import json
import sys
import logging
import sha
import os.path
import time
import uuid

logging.basicConfig(format='[%(asctime)-15s] [%(levelname)s]: %(message)s', level=logging.DEBUG)
log = logging.getLogger('')

PIECE_SIZE = 512 * 1024
MAX_PAR = 256

def scriptPath(name):
  return os.path.join(os.path.dirname(os.path.realpath(__file__)), name)

def getSize(url):
  o = subprocess.check_output(['bash', scriptPath('get-size.sh'), url])
  return long(o)

def manifestName(url):
  return '%s.json' % sha.new(url).hexdigest()

def genManifest(size):
  pieces = []
  for i in xrange(0, size, PIECE_SIZE):
    pieces.append({
      'done': False,
      'start': i,
      'end': min(size - 1, i + PIECE_SIZE - 1)
    })
  return pieces

def saveManifest(manifest, url):
  with open(manifestName(url), 'w') as f:
    f.write(json.dumps(manifest))

def loadManifest(url):
  p = manifestName(url)
  if os.path.isfile(p):
    log.info('Manifest exists, loading %s', p)
    with open(p, 'r') as f:
      return json.loads(f.read())
  else:
    mf = genManifest(getSize(url))
    log.info('Generating manifest to %s', manifestName(url))
    saveManifest(mf, url)
    return mf

def fileName(url):
  return url.split('/')[-1].split('?')[0]

def createFile(url, size):
  p = fileName(url)
  if os.path.isfile(p):
    log.info('%s exists', p)
  else:
    with open(p, 'wb') as f:
      f.truncate(size)
    log.info('Created %s', p)

def loop(manifest, url, tasks):
  hasChange = False
  for t in tasks:
    p = t['proc']
    if p.poll() is not None:
      if p.returncode == 0:
        manifest[t['idx']]['done'] = True
        hasChange = True
      else:
        log.error('Piece %d failed with code %d', t['idx'], p.returncode)
      t['done'] = True
  if hasChange:
    saveManifest(manifest, url)

  tasks = [t for t in tasks if not t['done']]
  running = [t['idx'] for t in tasks]
  for i in xrange(len(manifest)):
    if not manifest[i]['done'] and len(tasks) < MAX_PAR and i not in running:
      start = manifest[i]['start']
      end = manifest[i]['end']
      tasks.append({
        'idx': i,
        'proc': subprocess.Popen(['bash', scriptPath('get-piece.sh'), url,
          str(start), str(end), str(PIECE_SIZE), fileName(url), str(start / PIECE_SIZE)]),
        'start': start,
        'done': False
      })

  return (manifest, tasks)

def getProgress(manifest):
  done = len([x for x in manifest if x['done']])
  total = len(manifest)
  return (done, total)

def logProgress(manifest):
  done, total = getProgress(manifest)
  log.info('%d / %d done (%.2f%%)', done, total, 100.0 * done / total)

def main():
  url = sys.argv[1]
  mf = loadManifest(url)
  size = mf[-1]['end'] + 1
  log.info('Size is %.2f GB (%d)', float(size) / 1024 / 1024 / 1024, size)
  createFile(url, size)

  tasks = []
  n = 0
  while True:
    mf, tasks = loop(mf, url, tasks)
    if n % 5 == 0:
      logProgress(mf)
    time.sleep(1)
    n += 1
    done, total = getProgress(mf)
    if done == total:
      log.info('Completed')
      break

main()
