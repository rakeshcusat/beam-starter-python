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
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.core import DoFn
from apache_beam.transforms import window
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.userstate import on_timer
from colorama import Fore, Back, Style, init


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

    # Repeated data
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data2', TIMESTAMP_FIELD: BASE_TIMESTAMP},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data5', TIMESTAMP_FIELD: BASE_TIMESTAMP},

    {KEY_FIELD: 'key1', VALUE_FIELD: 'data6', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data7', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data8', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data9', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data10', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},

    # Repeated data
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data6', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data8', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data10', TIMESTAMP_FIELD: BASE_TIMESTAMP + 1 * ONE_MINUTE},

    {KEY_FIELD: 'key1', VALUE_FIELD: 'data11', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data12', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data13', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},

    # Repeated data
    {KEY_FIELD: 'key1', VALUE_FIELD: 'data12', TIMESTAMP_FIELD: BASE_TIMESTAMP + 2 * ONE_MINUTE},
]

init(autoreset=True)


class DeDuplicateStatefulDoFn(beam.DoFn):
  """
  This is a stateful processing class which is deduplicating the data for each window.
  """

  # Create the spec for SetState
  BUFFER_STATE = userstate.SetStateSpec(name='buffer', coder=StrUtf8Coder())

  # Create a timer spec baesd on the watermark. There are other timer spec available too.
  EXPIRY_TIMER = userstate.TimerSpec(name='expiry_timer', time_domain=TimeDomain.WATERMARK) 


  def process(self, 
              element: tuple[str, str], 
              buffer_state=beam.DoFn.StateParam(BUFFER_STATE),
              watermark_timer=DoFn.TimerParam(EXPIRY_TIMER),
              window=DoFn.WindowParam,
              ):
    unused_key, value = element

    # Setting the timer to trigger at the end of the window.
    watermark_timer.set(window.end)

    # Add  the value in state so we can retrieve it later.
    buffer_state.add(value)
  
  @on_timer(EXPIRY_TIMER)
  def expiry_callback(self,
                      buffer_state=beam.DoFn.StateParam(BUFFER_STATE)):
     print(Fore.BLUE + "Timer: timer callback, time to emit data")
     all_values = buffer_state.read()
     yield all_values

def print_elements(elements):
   """
   This is a utility function which is mainly used to print elements.
   """
   print(Fore.GREEN + f"Items: {", ".join(elements)}")


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
            | "Add one minute FIXED window" >> beam.WindowInto(window.FixedWindows(ONE_MINUTE))
            | "Set key and value" >> beam.Map(lambda item: (item[KEY_FIELD], item[VALUE_FIELD]))
            | "Stateful processing" >> beam.ParDo(DeDuplicateStatefulDoFn())
            | "Print elements" >> beam.Map(print_elements)
        )

        # Used for testing only.
        # test(elements)
