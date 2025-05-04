# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from typing import Callable
from typing import Optional
from collections.abc import Generator

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


BASE_TIMESTAMP = 1746339915
ONE_MINUTE = 60
NAME_FIELD = 'name'
TIMESTAMP_FIELD = 'timestamp'

INPUT_DATA = [
    {'name': 'data1', 'timestamp': BASE_TIMESTAMP},
    {'name': 'data2', 'timestamp': BASE_TIMESTAMP},
    {'name': 'data3', 'timestamp': BASE_TIMESTAMP},
    {'name': 'data4', 'timestamp': BASE_TIMESTAMP},
    {'name': 'data5', 'timestamp': BASE_TIMESTAMP},
    {'name': 'data6', 'timestamp': BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {'name': 'data7', 'timestamp': BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {'name': 'data8', 'timestamp': BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {'name': 'data9', 'timestamp': BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {'name': 'data10', 'timestamp': BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {'name': 'data11', 'timestamp': BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {'name': 'data12', 'timestamp': BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {'name': 'data13', 'timestamp': BASE_TIMESTAMP + 2 * ONE_MINUTE},
]


def split_words(input_string_list: str) ->  list[str]:
    result = []
    print(f"input_string_list type: {type(input_string_list)}")

    for input_string in input_string_list:
        for item in input_string.split(','):
            result.append(item)
    return result

def run(
    input_text: str,
    beam_options: Optional[PipelineOptions] = None,
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    with beam.Pipeline(options=beam_options) as pipeline:
        elements = (
            pipeline
            | "Create elements" >> beam.Create(INPUT_DATA)
            | 'With timestamps' >> beam.Map(
            lambda item: beam.window.TimestampedValue(item[NAME_FIELD], 
                                                      item[TIMESTAMP_FIELD]))
            | "Print elements" >> beam.Map(print)
        )

        # Used for testing only.
        test(elements)
