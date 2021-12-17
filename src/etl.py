import sys
import logging
from logging.handlers import RotatingFileHandler
from glob import glob
import configparser

import pandas as pd


def extract_institutions(path):
    institutions = pd.read_json(path).set_index('id')
    return institutions


def extract_submissions(path):
    submissions = pd.concat(map(pd.read_json, glob(path))).reset_index()
    return submissions


def process_institutions(df, path_root):
    df.to_parquet('{}/institutions.parquet'.format(path_root))


def process_subjects(df, path_root):
    subjects_json = [{'institution': submission_row.institution,
                      'year': submission_row.year,
                      'name': subject_row['name'],
                      'academic_papers': subject_row['academic_papers'],
                      'students_total': subject_row['students_total'],
                      'student_rating': subject_row['student_rating']
                      }
                     for submission_row in df.itertuples() for subject_row in submission_row.subjects]
    subjects = pd.DataFrame(subjects_json)
    subjects.to_parquet('{}/subjects.parquet'.format(path_root))


def process_submissions(df, path_root):
    df.drop('subjects', axis=1, inplace=True)
    df.to_parquet('{}/submissions.parquet'.format(path_root), index=True)


def main(config):
    logging.info('extracting institutions')
    institutions_df = extract_institutions(path=config['DEFAULT']['institutions_path'])
    logging.info('extracted institutions')

    logging.info('extracting submissions')
    submissions_df = extract_submissions(path=config['DEFAULT']['submissions_path'])
    logging.info('extracted submissions')

    logging.info('processing institutions')
    process_institutions(df=institutions_df, path_root=config['DEFAULT']['output_root'])
    logging.info('processed institutions')

    logging.info('processing subjects')
    process_subjects(df=submissions_df, path_root=config['DEFAULT']['output_root'])
    logging.info('processed subjects')

    logging.info('processing submissions')
    process_submissions(df=submissions_df, path_root=config['DEFAULT']['output_root'])
    logging.info('processed submissions')


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('etl.cfg')

    module_name = __file__.split('/')[-1]

    handlers = [RotatingFileHandler('logs/{}.log'.format(module_name), mode='a', maxBytes=1000000, backupCount=10),
                logging.StreamHandler(stream=sys.stdout)]

    logging.basicConfig(level=logging.INFO,
                        handlers=handlers,
                        datefmt='%Y-%m-%d %H:%M',
                        format='%(asctime)s %(name)-8s %(module)-12s %(levelname)-8s %(message)s')

    logging.info(' '.join(sys.argv))

    logging.getLogger("pika").setLevel(logging.WARNING)

    main(config=config)
