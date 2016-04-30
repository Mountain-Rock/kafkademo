package io.github.binglau;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 类BingPartitioner.java的实现描述：TODO:类实现描述
 *
 * @author bingjian.lbj 2016-04-30 下午7:31
 */
public class BingPartitioner implements Partitioner {

    public BingPartitioner() {

    }

    public BingPartitioner(VerifiableProperties props) {

    }

    public int partition(Object key, int partitionCount) {
        return Integer.valueOf((String) key) % partitionCount;
    }
}
