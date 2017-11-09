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
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.filter.predicate.EqualsPredicateEvaluatorFactory;
import com.linkedin.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.startree.StarTreeNode;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator based on star tree index.
 * <p>High-level algorithm:
 * <ul>
 *   <li>Traverse the filter tree and generate the map from predicate column to set of matching dictionary ids</li>
 *   <li>
 *     Traverse the star tree index, try to match as many predicates as possible, add the matching documents into a
 *     bitmap, and keep track of the remaining predicate columns and group-by columns
 *     <ul>
 *       <li>If we have predicates on the current dimension, add nodes associated with the matching dictionary ids</li>
 *       <li>If we don't have predicate but have group-by on the current dimension, add all non-star nodes</li>
 *       <li>If no predicate or group-by on the current dimension, use star node if exists, or all non-star nodes</li>
 *       <li>If all predicates and group-by are matched, add the aggregated document</li>
 *       <li>If we have remaining predicates or group-by at leaf node, add the documents from start to end</li>
 *       <li>
 *         If we have remaining predicates at leaf node, store the column because we need separate
 *         {@link BaseFilterOperator} for it
 *       </li>
 *       <li>Generate a {@link BitmapBasedFilterOperator} using the matching documents bitmap</li>
 *     </ul>
 *   </li>
 *   <li>
 *     For each remaining predicate column, use the set of matching dictionary ids to construct an IN
 *     {@link PredicateEvaluator}, and generate a {@link BaseFilterOperator} on it
 *   </li>
 *   <li>Conjoin all {@link BaseFilterOperator}s with AND if we have multiple of them</li>
 * </ul>
 */
public class StarTreeIndexBasedFilterOperator extends BaseFilterOperator {
  /**
   * Helper class to wrap the information needed when traversing the star tree.
   */
  private static class SearchEntry {
    StarTreeNode _starTreeNode;
    Set<String> _remainingPredicateColumns;
    Set<String> _remainingGroupByColumns;

    SearchEntry(StarTreeNode starTreeNode, Set<String> remainingPredicateColumns, Set<String> remainingGroupByColumns) {
      _starTreeNode = starTreeNode;
      _remainingPredicateColumns = remainingPredicateColumns;
      _remainingGroupByColumns = remainingGroupByColumns;
    }
  }

  /**
   * Helper class to wrap the result from traversing the star tree.
   */
  private static class StarTreeResult {
    final ImmutableRoaringBitmap _matchedDocIds;
    final Set<String> _remainingPredicateColumns;

    StarTreeResult(ImmutableRoaringBitmap matchedDocIds, Set<String> remainingPredicateColumns) {
      _matchedDocIds = matchedDocIds;
      _remainingPredicateColumns = remainingPredicateColumns;
    }
  }

  private static final String OPERATOR_NAME = "StarTreeIndexBasedFilterOperator";
  // If (number of matching dictionary ids * threshold) > (number of child nodes), use scan to traverse nodes instead of
  // binary search on each dictionary id
  private static final int USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD = 10;

  private final IndexSegment _indexSegment;
  // Map from column to matching dictionary ids
  private final Map<String, IntSet> _matchingDictIdsMap;
  // Set of group-by columns
  private final Set<String> _groupByColumns;

  boolean _resultEmpty = false;

  public StarTreeIndexBasedFilterOperator(IndexSegment indexSegment, BrokerRequest brokerRequest,
      FilterQueryTree rootFilterNode) {
    _indexSegment = indexSegment;

    // Process the filter tree and get the map from column to a list of predicates on it
    Map<String, List<Predicate>> predicatesMap = new HashMap<>();
    if (rootFilterNode != null) {
      processFilterNode(rootFilterNode, predicatesMap);
    }

    _matchingDictIdsMap = new HashMap<>(predicatesMap.size());
    for (Map.Entry<String, List<Predicate>> entry : predicatesMap.entrySet()) {
      String columnName = entry.getKey();
      List<Predicate> predicates = entry.getValue();
      IntSet matchingDictIds = getMatchingDictIds(columnName, predicates);

      // If no matching dictionary id found, the result will be empty, early terminate
      if (matchingDictIds.isEmpty()) {
        _resultEmpty = true;
        _groupByColumns = null;
        return;
      } else {
        _matchingDictIdsMap.put(columnName, matchingDictIds);
      }
    }

    _groupByColumns = RequestUtils.getAllGroupByColumns(brokerRequest.getGroupBy());

    // Remove columns with predicates from group-by columns because we won't use star node for that column
    _groupByColumns.removeAll(_matchingDictIdsMap.keySet());
  }

  /**
   * Helper method to process filter nodes recursively and add predicates into the map.
   */
  private void processFilterNode(FilterQueryTree filterNode, Map<String, List<Predicate>> predicatesMap) {
    List<FilterQueryTree> children = filterNode.getChildren();
    if (children != null) {
      for (FilterQueryTree child : children) {
        processFilterNode(child, predicatesMap);
      }
    } else {
      processLeafFilterNode(filterNode, predicatesMap);
    }
  }

  /**
   * Helper method to process the leaf filter node and add the predicate into the map.
   */
  private void processLeafFilterNode(FilterQueryTree leafFilterNode, Map<String, List<Predicate>> predicatesMap) {
    String column = leafFilterNode.getColumn();
    Predicate predicate = Predicate.newPredicate(leafFilterNode);
    List<Predicate> predicates = predicatesMap.get(column);
    if (predicates == null) {
      predicates = new ArrayList<>();
      predicatesMap.put(column, predicates);
    }
    predicates.add(predicate);
  }

  /**
   * Helper method to get a set of matching dictionary ids from a list of predicates conjoined with AND.
   * <ul>
   *   <li>
   *     We sort all predicates with priority: EQ > IN > RANGE > NOT_IN/NEQ > REGEXP_LIKE so that we process the
   *     cheapest predicate first.
   *   </li>
   *   <li>
   *     For the first predicate, we get all the matching dictionary ids, then apply them to other predicates to get the
   *     final set of matching dictionary ids.
   *   </li>
   *   <li>
   *     If there is no matching dictionary id for any column, set the result empty flag and early terminate.
   *   </li>
   * </ul>
   */
  private IntSet getMatchingDictIds(String columnName, List<Predicate> predicates) {
    // Sort the predicates so that we process the cheapest first
    Collections.sort(predicates, new Comparator<Predicate>() {
      @Override
      public int compare(Predicate o1, Predicate o2) {
        return Integer.compare(getPriority(o1), getPriority(o2));
      }

      private int getPriority(Predicate predicate) {
        switch (predicate.getType()) {
          case EQ:
            return 1;
          case IN:
            return 2;
          case RANGE:
            return 3;
          case NOT_IN:
          case NEQ:
            return 4;
          case REGEXP_LIKE:
          default:
            return 5;
        }
      }
    });

    IntSet matchingDictIds = new IntOpenHashSet();
    DataSource dataSource = _indexSegment.getDataSource(columnName);

    // Initialize matching dictionary ids with the first predicate
    Predicate firstPredicate = predicates.get(0);
    PredicateEvaluator firstPredicateEvaluator =
        PredicateEvaluatorProvider.getPredicateFunctionFor(firstPredicate, dataSource);
    for (int matchingDictId : firstPredicateEvaluator.getMatchingDictionaryIds()) {
      matchingDictIds.add(matchingDictId);
    }

    // Process other predicates
    int numPredicates = predicates.size();
    for (int i = 1; i < numPredicates; i++) {
      // We don't need to apply other predicates if all matching dictionary ids have already been removed
      if (matchingDictIds.isEmpty()) {
        return matchingDictIds;
      }
      Predicate predicate = predicates.get(i);
      PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dataSource);
      IntIterator iterator = matchingDictIds.iterator();
      while (iterator.hasNext()) {
        if (!predicateEvaluator.apply(iterator.nextInt())) {
          iterator.remove();
        }
      }
    }

    return matchingDictIds;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId blockId) {
    BaseFilterOperator filterOperator;
    if (_resultEmpty) {
      filterOperator = new EmptyFilterOperator();
    } else {
      List<BaseFilterOperator> childFilterOperators = getChildFilterOperators();
      if (childFilterOperators.size() == 1) {
        filterOperator = childFilterOperators.get(0);
      } else {
        filterOperator = new AndOperator(childFilterOperators);
      }
    }
    return filterOperator.nextFilterBlock(blockId);
  }

  @Override
  public boolean isResultEmpty() {
    return _resultEmpty;
  }

  /**
   * Helper method to get a list of child filter operators that match the matchingDictIdsMap.
   * <ul>
   *   <li>First go over the star tree and try to match as many columns as possible</li>
   *   <li>For the remaining columns, use other indexes to match them</li>
   * </ul>
   */
  private List<BaseFilterOperator> getChildFilterOperators() {
    StarTreeResult starTreeResult = traverseStarTree();
    List<BaseFilterOperator> childFilterOperators =
        new ArrayList<>(1 + starTreeResult._remainingPredicateColumns.size());

    // Inclusive end document id
    int endDocId = _indexSegment.getSegmentMetadata().getTotalDocs() - 1;

    // Add the bitmap of matching documents from star tree
    childFilterOperators.add(
        new BitmapBasedFilterOperator(new ImmutableRoaringBitmap[]{starTreeResult._matchedDocIds}, 0, endDocId, false));

    // Add remaining predicates
    for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
      IntSet matchingDictIds = _matchingDictIdsMap.get(remainingPredicateColumn);

      // For single matching dictionary id, create an EQ predicate evaluator for performance; otherwise, create an IN
      // predicate evaluator
      PredicateEvaluator predicateEvaluator;
      if (matchingDictIds.size() == 1) {
        predicateEvaluator =
            EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(matchingDictIds.iterator().nextInt());
      } else {
        predicateEvaluator = InPredicateEvaluatorFactory.newDictionaryBasedEvaluator(matchingDictIds);
      }

      // Create a filter operator for the column based on the index it has
      DataSource dataSource = _indexSegment.getDataSource(remainingPredicateColumn);
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      if (dataSourceMetadata.hasInvertedIndex()) {
        if (dataSourceMetadata.isSorted()) {
          childFilterOperators.add(
              new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, dataSource, 0, endDocId, false));
        } else {
          childFilterOperators.add(new BitmapBasedFilterOperator(predicateEvaluator, dataSource, 0, endDocId, false));
        }
      } else {
        childFilterOperators.add(new ScanBasedFilterOperator(predicateEvaluator, dataSource, 0, endDocId));
      }
    }

    return childFilterOperators;
  }

  /**
   * Helper method to traverse the star tree, get matching documents and keep track of all the predicate columns that
   * are not matched.
   */
  private StarTreeResult traverseStarTree() {
    MutableRoaringBitmap matchedDocIds = new MutableRoaringBitmap();
    Set<String> remainingPredicateColumns = new HashSet<>();

    StarTree starTree = _indexSegment.getStarTree();
    List<String> dimensionNames = starTree.getDimensionNames();
    StarTreeNode starTreeRootNode = starTree.getRoot();

    // Use BFS to traverse the star tree
    Queue<SearchEntry> queue = new LinkedList<>();
    queue.add(new SearchEntry(starTreeRootNode, _matchingDictIdsMap.keySet(), _groupByColumns));
    while (!queue.isEmpty()) {
      SearchEntry searchEntry = queue.remove();
      StarTreeNode starTreeNode = searchEntry._starTreeNode;

      // If all predicate columns and group-by columns are matched, we can use aggregated document
      if (searchEntry._remainingPredicateColumns.isEmpty() && searchEntry._remainingGroupByColumns.isEmpty()) {
        matchedDocIds.add(starTreeNode.getAggregatedDocId());
      } else {
        // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we cannot use
        // the aggregated document. Keep track of the remaining predicate columns for this node, and add the range of
        // documents for this node to the bitmap
        if (starTreeNode.isLeaf()) {
          remainingPredicateColumns.addAll(searchEntry._remainingPredicateColumns);
          matchedDocIds.add(starTreeNode.getStartDocId(), starTreeNode.getEndDocId());
        } else {
          // For non-leaf node, proceed to next level
          String nextDimension = dimensionNames.get(starTreeNode.getChildDimensionId());

          // If we have predicates on next level, add matching nodes to the queue
          if (searchEntry._remainingPredicateColumns.contains(nextDimension)) {
            Set<String> newRemainingPredicateColumns = new HashSet<>(searchEntry._remainingPredicateColumns);
            newRemainingPredicateColumns.remove(nextDimension);

            IntSet matchingDictIds = _matchingDictIdsMap.get(nextDimension);
            int numMatchingDictIds = matchingDictIds.size();
            int numChildren = starTreeNode.getNumChildren();

            // If number of matching dictionary ids is large, use scan instead of binary search
            if (numMatchingDictIds * USE_SCAN_TO_TRAVERSE_NODES_THRESHOLD > numChildren) {
              Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
              while (childrenIterator.hasNext()) {
                StarTreeNode childNode = childrenIterator.next();
                if (matchingDictIds.contains(childNode.getDimensionValue())) {
                  queue.add(
                      new SearchEntry(childNode, newRemainingPredicateColumns, searchEntry._remainingGroupByColumns));
                }
              }
            } else {
              IntIterator iterator = matchingDictIds.iterator();
              while (iterator.hasNext()) {
                int matchingDictId = iterator.nextInt();
                StarTreeNode childNode = starTreeNode.getChildForDimensionValue(matchingDictId);

                // Child node might be null because the matching dictionary id might not exist under this branch
                if (childNode != null) {
                  queue.add(
                      new SearchEntry(childNode, newRemainingPredicateColumns, searchEntry._remainingGroupByColumns));
                }
              }
            }
          } else {
            // If we don't have predicate or group-by on next level, use star node if exists
            Set<String> newRemainingGroupByColumns;
            if (!searchEntry._remainingGroupByColumns.contains(nextDimension)) {
              StarTreeNode starNode = starTreeNode.getChildForDimensionValue(StarTreeNode.ALL);
              if (starNode != null) {
                queue.add(new SearchEntry(starNode, searchEntry._remainingPredicateColumns,
                    searchEntry._remainingGroupByColumns));
                continue;
              }
              newRemainingGroupByColumns = searchEntry._remainingGroupByColumns;
            } else {
              newRemainingGroupByColumns = new HashSet<>(searchEntry._remainingGroupByColumns);
              newRemainingGroupByColumns.remove(nextDimension);
            }

            // Add all non-star nodes to the queue
            Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
            while (childrenIterator.hasNext()) {
              StarTreeNode childNode = childrenIterator.next();
              if (childNode.getDimensionValue() != StarTreeNode.ALL) {
                queue.add(
                    new SearchEntry(childNode, searchEntry._remainingPredicateColumns, newRemainingGroupByColumns));
              }
            }
          }
        }
      }
    }

    return new StarTreeResult(matchedDocIds, remainingPredicateColumns);
  }
}
