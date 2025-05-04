# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from collections.abc import Generator

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
            | "Create elements" >> beam.Create(['Hello', 'world', input_text])
            | "Split words" >> beam.FlatMap(split_words)
            # | "Apply timestamp" >> beam.
            | "Print elements" >> beam.Map(print)
        )

        # Used for testing only.
        test(elements)
