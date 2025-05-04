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

import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import userstate


BASE_TIMESTAMP = int(time.time())
ONE_MINUTE = 60

TIMESTAMP_FIELD = 'TIMESTAMP'
KEY_FIELD = 'KEY'
VALUE_FIELD = 'VALUE'

INPUT_DATA = [
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data1', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data2', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data3', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data4', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data5', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data6', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data7', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data8', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data9', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data10', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data11', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data12', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data13', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
]


class IndexAssigningStatefulDoFn(beam.DoFn):
  """
  This is a stateful processing class which is providing an index to each incoming message.
  """
  INDEX_STATE = userstate.CombiningValueStateSpec(name='index', coder=sum)

  def process(self, element, index=beam.DoFn.StateParam(INDEX_STATE)):
    unused_key, value = element
    current_index = index.read()
    index.add(1)
    yield (value, current_index)



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
            lambda item: beam.window.TimestampedValue(value=item, 
                                                      timestamp=item[TIMESTAMP_FIELD]))
            | "Set key and value" >> beam.Map(lambda item: (item[KEY_FIELD], item[VALUE_FIELD]))
            | "Stateful processing" >> beam.ParDo(IndexAssigningStatefulDoFn())                                          
            | "Print elements" >> beam.Map(print)
        )

        # Used for testing only.
        test(elements)
