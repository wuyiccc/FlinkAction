package com.wuyiccc.bigdata.flinkaction.stateprocessor;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

/**
 * @author wuyiccc
 * @date 2022/3/5 22:24
 * 读取保存点
 */
public class StateProcessorReadDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(env, "./file/savepoint/savepoint-1", new MemoryStateBackend());

        DataSet<Integer> listState = savepoint.readListState("uid1", "state1", Types.INT);
        listState.print();
    }
}
