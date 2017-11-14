/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.query;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Aggregation operator that utilizes metadata for serving aggregation queries.
 */
public class MetadataBasedAggregationOperator implements Operator {
  private final AggregationFunctionContext[] _aggregationFunctionContexts;
  private final Map<String, Dictionary> _dictionaryMap;
  private final SegmentMetadata _segmentMetadata;
  private ExecutionStatistics _executionStatistics;

  /**
   * Constructor for the class.
   *
   * @param aggregationFunctionContexts Aggregation function contexts.
   * @param segmentMetadata Segment metadata.
   * @param dictionaryMap Map of column to its dictionary.
   */
  public MetadataBasedAggregationOperator(AggregationFunctionContext[] aggregationFunctionContexts,
      SegmentMetadata segmentMetadata, Map<String, Dictionary> dictionaryMap) {
    _aggregationFunctionContexts = aggregationFunctionContexts;
    _dictionaryMap = dictionaryMap;
    _segmentMetadata = segmentMetadata;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    int numAggregationFunctions = _aggregationFunctionContexts.length;
    List<Object> aggregationResults = new ArrayList<>(numAggregationFunctions);
    int totalRawDocs = _segmentMetadata.getTotalRawDocs();

    for (AggregationFunctionContext aggregationFunctionContext : _aggregationFunctionContexts) {
      AggregationFunction function = aggregationFunctionContext.getAggregationFunction();
      AggregationFunctionFactory.AggregationFunctionType functionType =
          AggregationFunctionFactory.AggregationFunctionType.valueOf(function.getName().toUpperCase());
      String column = aggregationFunctionContext.getAggregationColumns()[0];
      Dictionary dictionary = _dictionaryMap.get(column);
      AggregationResultHolder resultHolder;
      switch (functionType) {
        case COUNT:
          resultHolder = new DoubleAggregationResultHolder(totalRawDocs);
          break;
        case MAX:
          resultHolder = new DoubleAggregationResultHolder(dictionary.getDoubleValue(dictionary.length() - 1));
          break;
        case MIN:
          resultHolder = new DoubleAggregationResultHolder(dictionary.getDoubleValue(0));
          break;
        case MINMAXRANGE:
          double max = dictionary.getDoubleValue(dictionary.length() - 1);
          double min = dictionary.getDoubleValue(0);
          resultHolder = new ObjectAggregationResultHolder();
          resultHolder.setValue(new MinMaxRangePair(min, max));
          break;
        default:
          throw new UnsupportedOperationException(
              "Metadata based aggregation operator does not support function " + function.getName());
      }
      aggregationResults.add(function.extractAggregationResult(resultHolder));
    }

    // Create execution statistics. Set numDocsScanned to totalRawDocs for backward compatibility.
    _executionStatistics =
        new ExecutionStatistics(totalRawDocs, 0/*numEntriesScannedInFilter*/, 0/*numEntriesScannedPostFilter*/,
            totalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctionContexts, aggregationResults, false);
  }

  @Override
  public Block nextBlock(BlockId blockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
