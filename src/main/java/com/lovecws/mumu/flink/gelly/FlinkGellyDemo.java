package com.lovecws.mumu.flink.gelly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: mumu-flink
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-12-03 16:41
 **/
public class FlinkGellyDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<String, Long>> vertices = new ArrayList<>();
        vertices.add(new Vertex<String, Long>("1", 1L));
        vertices.add(new Vertex<String, Long>("2", 2L));
        vertices.add(new Vertex<String, Long>("3", 3L));
        vertices.add(new Vertex<String, Long>("4", 4L));

        List<Edge<String, Double>> edges = new ArrayList<>();
        edges.add(new Edge<String, Double>("1", "2", 0.5));
        edges.add(new Edge<String, Double>("2", "3", 2.5));
        edges.add(new Edge<String, Double>("1", "4", 2.0));
        Graph<String, Long, Double> graph = Graph.fromCollection(vertices, edges, env);
        long numberOfEdges = graph.numberOfEdges();
        System.out.println(numberOfEdges);
        DataSet<Tuple2<String, Long>> tuple2DataSet = graph.reduceOnNeighbors(new ReduceNeighborsFunction<Long>() {
            @Override
            public Long reduceNeighbors(Long firstNeighborValue, Long secondNeighborValue) {
                return firstNeighborValue + secondNeighborValue;
            }
        }, EdgeDirection.ALL);
        tuple2DataSet.print();

        //点过滤 查询
        Graph<String, Long, Double> filterGraph = graph.filterOnVertices(new FilterFunction<Vertex<String, Long>>() {
            @Override
            public boolean filter(Vertex<String, Long> value) throws Exception {
                return value.f1 == 1L;
            }
        });
        filterGraph.getEdges().print();
        filterGraph.getVertices().print();

        graph.getEdgesAsTuple3().print();
        graph.getEdges().print();
    }
}
