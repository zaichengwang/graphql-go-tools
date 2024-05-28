package plan

import (
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/astvisitor"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/operationreport"
	"math/rand"
)

const typeNameField = "__typename"

type DataSourceFilter struct {
	operation  *ast.Document
	definition *ast.Document
	report     *operationreport.Report

	nodes *NodeSuggestions

	enableSelectionReasons bool
}

func NewDataSourceFilter(operation, definition *ast.Document, report *operationreport.Report) *DataSourceFilter {
	return &DataSourceFilter{
		operation:  operation,
		definition: definition,
		report:     report,
	}
}

func (f *DataSourceFilter) EnableSelectionReasons() {
	f.enableSelectionReasons = true
}

func (f *DataSourceFilter) FilterDataSources(dataSources []DataSource, existingNodes *NodeSuggestions, hints ...NodeSuggestionHint) (used []DataSource, suggestions *NodeSuggestions) {
	var dsInUse map[DSHash]struct{}

	suggestions, dsInUse = f.findBestDataSourceSet(dataSources, existingNodes, hints...)
	if f.report.HasErrors() {
		return
	}

	used = make([]DataSource, 0, len(dsInUse))
	for i := range dataSources {
		_, inUse := dsInUse[dataSources[i].Hash()]
		if inUse {
			used = append(used, dataSources[i])
		}
	}

	return used, suggestions
}

func (f *DataSourceFilter) findBestDataSourceSet(dataSources []DataSource, existingNodes *NodeSuggestions, hints ...NodeSuggestionHint) (*NodeSuggestions, map[DSHash]struct{}) {
	f.nodes = f.collectNodes(dataSources, existingNodes)
	if f.report.HasErrors() {
		return nil, nil
	}

	// f.nodes.printNodes("initial nodes")

	f.applySuggestionHints(hints)
	// f.nodes.printNodes("nodes after applying hints")

	hasDuplicates := f.selectUniqueNodes()

	if hasDuplicates {
		// assign datasource config to the nodes
		// make datasource config to hash->config map
		dsConfigMap := map[DSHash]DataSource{}
		for _, dsConfig := range dataSources {
			if dsConfig.IsRolloutEnabled() {
				dsConfigMap[dsConfig.Hash()] = dsConfig
			}
		}
		for i := range f.nodes.items {
			dsHash := f.nodes.items[i].DataSourceHash
			if _, ok := dsConfigMap[dsHash]; !ok {
				continue
			}
			// now we only support Query and Mutation root nodes
			if f.nodes.items[i].TypeName != "Query" && f.nodes.items[i].TypeName != "Mutation" {
				continue
			}
			f.nodes.items[i].isPrimaryDataSource = dsConfigMap[dsHash].IsPrimaryDataSource()
			f.nodes.items[i].rolloutPercentage = dsConfigMap[dsHash].RolloutPercentage()
			f.nodes.items[i].isRolloutEnabled = dsConfigMap[dsHash].IsRolloutEnabled()
			f.nodes.items[i].DataSourceID = dsConfigMap[dsHash].Id()
			// override by the node specific rollout configuration
			for _, nodeRolloutConfig := range dsConfigMap[dsHash].NodeRolloutConfigs() {
				if nodeRolloutConfig.NodeName == f.nodes.items[i].FieldName && nodeRolloutConfig.NodeType == f.nodes.items[i].TypeName {
					f.nodes.items[i].rolloutPercentage = nodeRolloutConfig.RolloutPercentage
					f.nodes.items[i].isRolloutEnabled = nodeRolloutConfig.IsRolloutEnabled
					break
				}
			}
		}
		// select root node based on the rollout configuration
		f.selectFromDuplicateRootNodes()

	}

	// f.nodes.printNodes("unique nodes")
	f.selectDuplicateNodes(false)
	// f.nodes.printNodes("duplicate nodes")
	f.selectDuplicateNodes(true)
	// f.nodes.printNodes("duplicate nodes after second run")

	uniqueDataSourceHashes := f.nodes.populateHasSuggestions()

	f.isResolvable(f.nodes)
	if f.report.HasErrors() {
		return nil, nil
	}

	return f.nodes, uniqueDataSourceHashes
}

func (f *DataSourceFilter) collectNodes(dataSources []DataSource, existingNodes *NodeSuggestions, hints ...NodeSuggestionHint) (nodes *NodeSuggestions) {
	secondaryRun := existingNodes != nil

	walker := astvisitor.NewWalker(32)
	visitor := &collectNodesVisitor{
		operation:           f.operation,
		definition:          f.definition,
		walker:              &walker,
		dataSources:         dataSources,
		secondaryRun:        secondaryRun,
		nodes:               existingNodes,
		hints:               hints,
		saveSelectionReason: f.enableSelectionReasons,
	}
	walker.RegisterEnterDocumentVisitor(visitor)
	walker.RegisterFieldVisitor(visitor)
	walker.Walk(f.operation, f.definition, f.report)
	return visitor.nodes
}

func (f *DataSourceFilter) isResolvable(nodes *NodeSuggestions) {
	walker := astvisitor.NewWalker(32)
	visitor := &nodesResolvableVisitor{
		operation:  f.operation,
		definition: f.definition,
		walker:     &walker,
		nodes:      nodes,
	}
	walker.RegisterEnterFieldVisitor(visitor)
	walker.Walk(f.operation, f.definition, f.report)
}

const (
	ReasonStage1Unique                = "stage1: unique"
	ReasonStage1SameSourceParent      = "stage1: same source parent of unique node"
	ReasonStage1SameSourceLeafChild   = "stage1: same source leaf child of unique node"
	ReasonStage1SameSourceLeafSibling = "stage1: same source leaf sibling of unique node"

	ReasonStage2SameSourceNodeOfSelectedParent  = "stage2: node on the same source as selected parent"
	ReasonStage2SameSourceNodeOfSelectedChild   = "stage2: node on the same source as selected child"
	ReasonStage2SameSourceNodeOfSelectedSibling = "stage2: node on the same source as selected sibling"

	ReasonStage3SelectAvailableNode = "stage3: select first available node"

	ReasonStage3SelectBasedOnRolloutConfig = "stage3: select based on rollout config"

	ReasonKeyRequirementProvidedByPlanner = "provided by planner as required by @key"
)

func (f *DataSourceFilter) applySuggestionHints(hints []NodeSuggestionHint) {
	if len(hints) == 0 {
		return
	}

	for _, hint := range hints {
		treeNodeID := TreeNodeID(hint.fieldRef)
		treeNode, ok := f.nodes.responseTree.Find(treeNodeID)
		if !ok {
			continue
		}

		itemIndexes := treeNode.GetData()
		for _, itemIdx := range itemIndexes {
			if f.nodes.items[itemIdx].DataSourceHash != hint.dsHash {
				if f.nodes.items[itemIdx].Selected {
					// if the node was already selected by another datasource
					// we unselect it
					f.nodes.items[itemIdx].Selected = false
					f.nodes.items[itemIdx].SelectionReasons = nil
				}
			} else {
				f.nodes.items[itemIdx].selectWithReason(ReasonKeyRequirementProvidedByPlanner, f.enableSelectionReasons)
			}
		}
	}
}

func (f *DataSourceFilter) selectFromDuplicateRootNodes() bool {

	for i := range f.nodes.items {
		if !f.nodes.items[i].IsRootNode {
			continue
		}
		if f.nodes.items[i].TypeName != "Query" && f.nodes.items[i].TypeName != "Mutation" {
			continue
		}
		if f.nodes.items[i].Selected {
			continue
		}

		nodeDuplicates := f.nodes.duplicatesOf(i)
		nodeDuplicates = append(nodeDuplicates, i)
		if len(nodeDuplicates) != 2 {
			continue
		}

		// Initialize rollout variables
		rolloutEnabled := false
		rolloutPercentageTotal := 0
		primaryNodeIndex := -1
		// Check if rollout is enabled and calculate total rollout percentage
		for _, nodeIndex := range nodeDuplicates {
			currentNode := f.nodes.items[nodeIndex]
			if currentNode.isRolloutEnabled && (currentNode.rolloutPercentage > 0 || currentNode.isPrimaryDataSource) {
				rolloutEnabled = true
				rolloutPercentageTotal += currentNode.rolloutPercentage
				if currentNode.isPrimaryDataSource {
					primaryNodeIndex = nodeIndex
				}
			}
		}
		if rolloutEnabled {
			if primaryNodeIndex != -1 {
				// Assign the remaining rollout percentage to the primary node
				f.nodes.items[primaryNodeIndex].rolloutPercentage = 100 - rolloutPercentageTotal
			} else {
				//queryExecutionReport.QueryPlanningError = errors.New("primary node not found " + fmt.Sprintf("%v", nodes[i].Path))
				continue
			}

			// Select a node based on rollout percentage
			randomNumber := rand.Intn(101)
			selectedNodeIndex := primaryNodeIndex
			if randomNumber > f.nodes.items[primaryNodeIndex].rolloutPercentage {
				// Select the non-primary node
				for _, nodeIndex := range nodeDuplicates {
					if nodeIndex != primaryNodeIndex {
						selectedNodeIndex = nodeIndex
						break
					}
				}
			}

			// Apply the selection
			f.nodes.items[selectedNodeIndex].selectWithReason(ReasonStage3SelectBasedOnRolloutConfig, f.enableSelectionReasons)
			// we expect only has 1 root node
			return true
			//queryExecutionReport.PlanDecisionMap[nodes[selectedNodeIndex].Path] = nodes[selectedNodeIndex].DataSourceID
		}

	}

	return true
}

// selectUniqueNodes - selects nodes (e.g. fields) which are unique to a single datasource
// In addition we select:
//   - parent of such node if the node is a leaf and not nested under the fragment
//   - siblings nodes
func (f *DataSourceFilter) selectUniqueNodes() bool {

	hasDuplicate := false

	for i := range f.nodes.items {
		if f.nodes.items[i].Selected {
			continue
		}

		isNodeUnique := f.nodes.isNodeUnique(i)
		if !isNodeUnique {
			hasDuplicate = true
			continue
		}

		// unique nodes always have priority
		f.nodes.items[i].selectWithReason(ReasonStage1Unique, f.enableSelectionReasons)

		if !f.nodes.items[i].onFragment { // on a first stage do not select parent of nodes on fragments
			// if node parents of the unique node is on the same source, prioritize it too
			f.selectUniqNodeParentsUpToRootNode(i)
		}

		// if node has leaf children on the same source, prioritize them too
		children := f.nodes.childNodesOnSameSource(i)
		for _, child := range children {
			if f.nodes.isLeaf(child) && f.nodes.isNodeUnique(child) {
				f.nodes.items[child].selectWithReason(ReasonStage1SameSourceLeafChild, f.enableSelectionReasons)
			}
		}

		// prioritize leaf siblings of the node on the same source
		siblings := f.nodes.siblingNodesOnSameSource(i)
		for _, sibling := range siblings {
			if f.nodes.isLeaf(sibling) && f.nodes.isNodeUnique(sibling) {
				f.nodes.items[sibling].selectWithReason(ReasonStage1SameSourceLeafSibling, f.enableSelectionReasons)
			}
		}
	}
	return hasDuplicate
}

func (f *DataSourceFilter) selectUniqNodeParentsUpToRootNode(i int) {
	// When we have a chain of datasource child nodes, we should select every parent until we reach the root node
	// as root node is a starting point from where we could get all theese child nodes

	if f.nodes.items[i].IsRootNode {
		// no need to select parent of a root node here
		// as it could be resolved by itself
		return
	}

	current := i
	for {
		parentIdx, ok := f.nodes.parentNodeOnSameSource(current)
		if !ok {
			break
		}
		f.nodes.items[parentIdx].selectWithReason(ReasonStage1SameSourceParent, f.enableSelectionReasons)

		current = parentIdx
		if f.nodes.items[current].IsRootNode {
			break
		}
	}
}

// selectDuplicateNodes - selects nodes (e.g. fields) which are not unique to a single datasource,
// e.g. could be resolved by multiple datasources
// This method checks only nodes not already selected on the other datasource
// On a first run we are doing set of checks of surrounding nodes selection for the current analyzed node and each of its duplicates:
//   - check for selected parent of a current node or its duplicates
//   - check for selected childs of a current node or its duplicates
//   - check for selected siblings of a current node or its duplicates
//
// On a second run in additional to all the checks from the first run
// we select nodes which was not choosen by previous stages, so we just pick first available datasource
func (f *DataSourceFilter) selectDuplicateNodes(secondRun bool) {
	for i := range f.nodes.items {
		if f.nodes.items[i].Selected {
			continue
		}

		if f.nodes.isSelectedOnOtherSource(i) {
			continue
		}

		nodeDuplicates := f.nodes.duplicatesOf(i)

		// check for selected parent of a current node or its duplicates
		if f.checkNodeParent(i) {
			continue
		}
		if f.checkNodeDuplicates(nodeDuplicates, f.checkNodeParent) {
			continue
		}

		// check for selected childs of a current node or its duplicates
		if f.checkNodeChilds(i) {
			continue
		}
		if f.checkNodeDuplicates(nodeDuplicates, f.checkNodeChilds) {
			continue
		}

		// check for selected siblings of a current node or its duplicates
		if f.checkNodeSiblings(i) {
			continue
		}
		if f.checkNodeDuplicates(nodeDuplicates, f.checkNodeSiblings) {
			continue
		}

		// if after all checks node was not selected, select it
		// this could happen in case choises are fully equal
		if secondRun {
			// in case current node suggestion is an entity root node, and it contains key with disabled resolver
			// it makes such node less preferable for selection
			if f.nodes.items[i].LessPreferable {
				continue
			}

		} else {
			// Default selection when rollout is not enabled or there are not exactly two nodes
			f.nodes.items[i].selectWithReason(ReasonStage3SelectAvailableNode, f.enableSelectionReasons)
		}
	}
}

func (f *DataSourceFilter) checkNodeDuplicates(duplicates []int, callback func(nodeIdx int) (nodeIsSelected bool)) (nodeIsSelected bool) {
	for _, duplicate := range duplicates {
		if callback(duplicate) {
			nodeIsSelected = true
			break
		}
	}
	return
}

func (f *DataSourceFilter) checkNodeChilds(i int) (nodeIsSelected bool) {
	childs := f.nodes.childNodesOnSameSource(i)
	for _, child := range childs {
		if f.nodes.items[child].Selected {
			f.nodes.items[i].selectWithReason(ReasonStage2SameSourceNodeOfSelectedChild, f.enableSelectionReasons)
			nodeIsSelected = true
			break
		}
	}
	return
}

func (f *DataSourceFilter) checkNodeSiblings(i int) (nodeIsSelected bool) {
	siblings := f.nodes.siblingNodesOnSameSource(i)
	for _, sibling := range siblings {
		if f.nodes.items[sibling].Selected {
			f.nodes.items[i].selectWithReason(ReasonStage2SameSourceNodeOfSelectedSibling, f.enableSelectionReasons)
			nodeIsSelected = true
			break
		}
	}
	return
}

func (f *DataSourceFilter) checkNodeParent(i int) (nodeIsSelected bool) {
	parentIdx, ok := f.nodes.parentNodeOnSameSource(i)
	if ok && f.nodes.items[parentIdx].Selected {
		f.nodes.items[i].selectWithReason(ReasonStage2SameSourceNodeOfSelectedParent, f.enableSelectionReasons)
		nodeIsSelected = true
	}

	return
}
