package com.lovecws.mumu.flink.gelly;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.*;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

/**
 * @program: mumu-flink
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-12-03 17:11
 **/
public class GraphDemo {

    public void gridGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        boolean wrapEndpoints = false;

        int parallelism = 4;

        Graph<LongValue, NullValue, NullValue> graph = new GridGraph(env)
                .addDimension(2, wrapEndpoints)
                .addDimension(4, wrapEndpoints)
                .setParallelism(parallelism)
                .generate();

        graph.getEdges().print();
        graph.getVertices().print();
    }

    public void circulantGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long vertexCount = 5;

        Graph<LongValue, NullValue, NullValue> graph = new CirculantGraph(env, vertexCount)
                .addRange(1, 2)
                .generate();
        graph.getEdges().print();
        graph.getVertices().print();
    }

    public void completeGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long vertexCount = 5;

        Graph<LongValue, NullValue, NullValue> graph = new CompleteGraph(env, vertexCount)
                .generate();
        graph.getEdges().print();
        graph.getVertices().print();
    }

    public void cycleGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long vertexCount = 5;

        Graph<LongValue, NullValue, NullValue> graph = new CycleGraph(env, vertexCount)
                .generate();
        graph.getEdges().print();
        graph.getVertices().print();
    }

    public void echoGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long vertexCount = 5;
        long vertexDegree = 2;

        Graph<LongValue, NullValue, NullValue> graph = new EchoGraph(env, vertexCount, vertexDegree)
                .generate();
        graph.getEdges().print();
        graph.getVertices().print();
    }

    public void emptyGraph() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        long vertexCount = 5;

        Graph<LongValue, NullValue, NullValue> graph = new EmptyGraph(env, vertexCount)
                .generate();
        graph.getEdges().print();
        graph.getVertices().print();
    }

    public static void main(String[] args) throws Exception {
        GraphDemo graphDemo = new GraphDemo();
        //graphDemo.gridGraph();
        graphDemo.circulantGraph();
    }
}
