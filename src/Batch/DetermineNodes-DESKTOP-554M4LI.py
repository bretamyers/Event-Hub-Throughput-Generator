import sys
import math
import json
import copy
import DataFactory.PayloadFactory


def get_batch_specs(TargetThroughput:int, JsonFilePath:str=None) -> dict:
    print(f'Parameter - Target Throughput - {TargetThroughput}')
    payloadDefinitionDict = DataFactory.PayloadFactory.get_payload_definition(JsonFilePath)

    eventString = DataFactory.PayloadFactory.gen_payload(jsonAttributePathDict=copy.deepcopy(payloadDefinitionDict), maxValueFlag=True)
    print(json.dumps(eventString))

    # print(f'EventString with max values - {eventString}')
    print(f'Message size (bytes) - {sys.getsizeof(eventString)}')
    MaxMessageSizeBytes = sys.getsizeof(eventString)

    MaxThroughputPerNode = round(((1*1000*1000) / MaxMessageSizeBytes) * 0.8) #multiplying by 0.8 just to give a little headroom

    if TargetThroughput < MaxThroughputPerNode:
        NodeThroughput = TargetThroughput
    else:
        NodeThroughput = math.floor(TargetThroughput/(math.ceil(TargetThroughput/MaxThroughputPerNode)))

    print(f'Max Node Throughput - {MaxThroughputPerNode}')
    # print(f'Ideal Node Throughput - {NodeThroughput}')
    NumberOfNodes = 4 if math.floor(TargetThroughput/NodeThroughput) < 2 else 4 * math.floor(TargetThroughput/NodeThroughput) #default the number of nodes to 4
    # NodeThroughput = math.floor(NodeThroughput/4)
    print(f'Number of nodes - {NumberOfNodes}')
    print(f'Number of messages per sec in a batch per node - {NodeThroughput}')
    print(f'Message size (bytes) of a batch per node - {NodeThroughput * MaxMessageSizeBytes}')
    print(f'Target throughput Messages/Sec - {int(TargetThroughput)}')
    print(f'Basic Throughput Messages/Sec - {int(TargetThroughput/NodeThroughput) * NodeThroughput}')

    NodeMessageSpecList = list()
    NodesAboveAverage = int(TargetThroughput) - (int(TargetThroughput/NodeThroughput) * NodeThroughput)
    # print(NodesAboveAverage)
    MinNodeThroughput = math.floor(int(TargetThroughput) / (NumberOfNodes/4))
    MaxNodeThroughput = math.ceil(int(TargetThroughput) / (NumberOfNodes/4))
    for node in range(NumberOfNodes):
        if node%(NumberOfNodes/4) < NodesAboveAverage:
            NodeMessageSpecList.append({'NodeNum': str(int(node)+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(int(NodeThroughput)+1)})
        else:
            NodeMessageSpecList.append({'NodeNum': str(int(node)+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(NodeThroughput)})


    nodeBuckets = dict()
    for nodespec in NodeMessageSpecList:
        if nodespec['NodeSec'] in nodeBuckets.keys():
            nodeBuckets[nodespec['NodeSec']] += nodespec['NodeThroughput']
        else:
            nodeBuckets[nodespec['NodeSec']] = nodespec['NodeThroughput']
    print(f'Node Buckets (Node, Throughput) - {nodeBuckets}')

    batchSpecDict = {
                    'PayloadDefinitionDict': payloadDefinitionDict
                    ,'NumberOfNodes': NumberOfNodes
                    ,'MinNodeThroughput': MinNodeThroughput
                    ,'MaxNodeThroughput': MaxNodeThroughput
                    ,'NodeMessageSpecList': NodeMessageSpecList
                    }
                    
    return batchSpecDict

if __name__ == '__main__':

    TargetThroughput = 1#100001 #thoughput messages/sec

    batchSpecDict = get_batch_specs(TargetThroughput=TargetThroughput)

    print(json.dumps(batchSpecDict))

    # #Speed Test
    # import time
    # start = time.time()
    # batchList = list()
    # for _ in range(batchSpecDict['NodeThroughput']):
    #     batchList.append(gen_payload(jsonAttributePathList=[_ for _ in batchSpecDict['payloadDefinitionList']], maxValueFlag=False))
    # print(f'Duration to generate payload (sec) - {str(round(time.time() - start, 2))}')
    # # print(len(batchList))
    # # print(batchList)
