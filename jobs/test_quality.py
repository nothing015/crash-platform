import csv
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from pyspark.sql import SparkSession

import quality


class QualityJobTest(unittest.TestCase):
    def test_bad_rows_are_counted_quarantined_and_excluded(self):
        headers = [
            'COLLISION ID', 'CRASH DATE', 'CRASH TIME', 'BOROUGH',
            'ON STREET NAME', 'LATITUDE', 'LONGITUDE',
            *[column.replace('_', ' ').upper() for column in quality.COUNT_COLUMNS],
        ]
        rows = []
        for index in range(2_000):
            rows.append([
                str(index + 1), '01/02/2019', '12:00', 'MANHATTAN', 'MAIN ST',
                '40.7', '-73.9', *(['0'] * len(quality.COUNT_COLUMNS)),
            ])

        rows[0][2] = '25:63'
        rows[1][1] = '01/01/2000'
        rows[2][7] = '-1'
        rows[3][3] = 'NOT A BOROUGH'
        rows[4][0] = ''
        rows[5][0] = 'duplicate-id'
        rows[6][0] = 'duplicate-id'
        rows[7][5] = '100'
        rows[8][7] = 'not-a-number'

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, 'crashes.csv')
            results_dir = os.path.join(temp_dir, 'results')
            report_path = os.path.join(results_dir, 'quality_report.json')
            quarantine_path = os.path.join(temp_dir, 'quarantine')
            validated_path = os.path.join(temp_dir, 'validated')
            with open(input_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)

            with (
                patch.object(quality, 'RAW_FILE', input_path),
                patch.object(quality, 'RESULTS_DIR', results_dir),
                patch.object(quality, 'QUARANTINE_PATH', quarantine_path),
                patch.object(quality, 'VALIDATED_PATH', validated_path),
                redirect_stdout(StringIO()),
            ):
                spark = (SparkSession.builder
                         .appName('quality_job_test')
                         .config('spark.sql.shuffle.partitions', '2')
                         .getOrCreate())
                spark.sparkContext.setLogLevel('ERROR')
                quality.main()

            with open(report_path) as report_file:
                report = json.load(report_file)
            self.assertEqual(report['rows_in'], 2_000)
            self.assertEqual(report['rows_out'], 1_991)
            self.assertEqual(report['rows_quarantined'], 9)
            self.assertEqual(report['overall_status'], 'fail')
            self.assertEqual(len(report['rules']), 9)
            failed_rows = {rule['id']: rule['failed_rows'] for rule in report['rules']}
            self.assertEqual(failed_rows, {
                'required_schema': 0,
                'valid_crash_timestamp': 1,
                'plausible_timestamp_range': 1,
                'nonnegative_integer_counts': 2,
                'invalid_count_casts': 1,
                'valid_borough': 1,
                'present_collision_id': 1,
                'unique_collision_id': 2,
                'nyc_coordinates': 1,
            })
            self.assertEqual(
                report['invalid_casts_by_column']['number_of_persons_injured'], 1
            )

            spark = SparkSession.builder.appName('quality_job_test_assertions').getOrCreate()
            try:
                quarantine = spark.read.parquet(quarantine_path)
                self.assertEqual(quarantine.count(), 9)
                self.assertIn('failed_rules', quarantine.columns)
                missing_id = quarantine.filter(
                    (
                        quarantine.collision_id.isNull()
                        | (quality.F.trim(quarantine.collision_id) == '')
                    )
                    & quality.F.array_contains('failed_rules', 'present_collision_id')
                )
                self.assertEqual(missing_id.count(), 1)
                self.assertEqual(spark.read.parquet(validated_path).count(), 1_991)
            finally:
                spark.stop()


if __name__ == '__main__':
    unittest.main()
