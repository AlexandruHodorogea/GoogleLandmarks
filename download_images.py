import multiprocessing
import multiprocessing.pool
import os
from io import BytesIO
from urllib import request
import pandas as pd
import re
import tqdm
from PIL import Image

from gcs_wrapper import get_bucket, upload_from_data, get_downloaded_ids

# Global module vars
NUM_WORKERS = None
TARGET_SIZE = None
BUCKET_NAME = None
OUT_DIR = None
GLOBAL_BUCKET = None
GLOBAL_INIT = False
IDS_DOWNLOADED = None

def overwrite_urls(df):
    def reso_overwrite(url_tail, reso=TARGET_SIZE):
        pattern = 's[0-9]+'
        search_result = re.match(pattern, url_tail)
        if search_result is None:
            return url_tail
        else:
            return 's{}'.format(reso)

    def join_url(parsed_url, s_reso):
        parsed_url[-2] = s_reso
        return '/'.join(parsed_url)

    parsed_url = df.url.apply(lambda x: x.split('/'))
    train_url_tail = parsed_url.apply(lambda x: x[-2])
    resos = train_url_tail.apply(lambda x: reso_overwrite(x, reso=TARGET_SIZE))

    overwritten_df = pd.concat([parsed_url, resos], axis=1)
    overwritten_df.columns = ['url', 's_reso']
    df['url'] = overwritten_df.apply(lambda x: join_url(x['url'], x['s_reso']), axis=1)
    return df


def parse_data(df):
    key_url_list = [(line[0], line[1]) for line in df.values]
    return key_url_list
 
def key_to_path(key):
    return os.path.join(OUT_DIR, '{}.jpg'.format(key))

class Downloader(object):

    def __init__(self):
        self.bucket = get_bucket(BUCKET_NAME)

    def __call__(self, key_url):
        (key, url) = key_url

        if key in IDS_DOWNLOADED:
#             print('Image {} already exists. Skipping download.'.format(key))
            return 0

        filename = key_to_path(key)

        try:
            response = request.urlopen(url)
            image_data = response.read()
        except:
#             print('Warning: Failed to download image {}'.format(url))
            return 1
         
        if TARGET_SIZE:
          try:
              pil_image = Image.open(BytesIO(image_data))
          except:
              print('Warning: Failed to parse image {}'.format(key))
              return 1

          try:
              pil_image_rgb = pil_image.convert('RGB')
          except:
              print('Warning: Failed to convert image {} to RGB'.format(key))
              return 1

          try:
              pil_image_resize = pil_image_rgb.resize((TARGET_SIZE, TARGET_SIZE))
          except:
              print('Warning: Failed to resize image {}'.format(key))
              return 1
          
          try:
            image_data = BytesIO()
            pil_image_resize.save(image_data, format='JPEG')
            image_data = image_data.getvalue()
          except:
            print('Warning: Failed to save image {}'.format(filename))
            return 1
          
        try:
            content_type = response.headers['Content-Type']
            upload_from_data(image_data, filename, self.bucket, 
                content_type=content_type)
        except:
          print('Warning: Failed to upload image {}'.format(filename))
          return 1

        return 0
          
def loader(df):
    if not os.path.exists(OUT_DIR):
        os.mkdir(OUT_DIR)

    key_url_list = parse_data(df)
    pool = multiprocessing.pool.ThreadPool(processes=NUM_WORKERS)
    failures = sum(pool.imap_unordered(Downloader(), key_url_list))
    
    print('Total number of download failures: %s out of %s' % (failures, len(key_url_list)))
    pool.close()
    pool.terminate()


def main(df):
    if not GLOBAL_INIT:
        raise Exception("Call init function for download_images module")
    global IDS_DOWNLOADED
    if IDS_DOWNLOADED is None:
      IDS_DOWNLOADED = get_downloaded_ids(GLOBAL_BUCKET, OUT_DIR)
    if TARGET_SIZE:
      loader(overwrite_urls(df))
    else:
      loader(df)


def run_on_batches(df, batches_num=10):
    # run on batches
    ex_nums = df['id'].count()
    a = 0; b = int(ex_nums/batches_num); i = 0
    while a < ex_nums - 1:
        print('batch %s/%s' % (i, batches_num))
        main(df[a:b])
        a = b
        b = min(b + int(ex_nums/batches_num), ex_nums - 1)
        i += 1


def init(bucket_name, out_dir, num_workers, target_size):
    global BUCKET_NAME, OUT_DIR, NUM_WORKERS, TARGET_SIZE, GLOBAL_INIT, GLOBAL_BUCKET
    GLOBAL_INIT = True
    BUCKET_NAME = bucket_name
    OUT_DIR = out_dir
    NUM_WORKERS = num_workers
    TARGET_SIZE = target_size
    GLOBAL_BUCKET = get_bucket(BUCKET_NAME)

