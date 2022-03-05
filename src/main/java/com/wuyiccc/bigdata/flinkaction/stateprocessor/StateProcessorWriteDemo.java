package com.wuyiccc.bigdata.flinkaction.stateprocessor;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

/**
 * @author wuyiccc
 * @date 2022/3/5 22:01
 */
public class StateProcessorWriteDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6);
        BootstrapTransformation transformation = OperatorTransformation.bootstrapWith(input)
                .transform(new MySimpleBootstrapFunction());
        int maxParallelism = 128;
        Savepoint.create(new MemoryStateBackend(), maxParallelism)
                .withOperator("uid1", transformation)
                .write("./file/savepoint/savepoint-1");

        env.execute();
    }

    private static class MySimpleBootstrapFunction extends StateBootstrapFunction<Integer> {
        private ListState<Integer> state;

        // 将给定值写入状态, 每个记录都会调用此方法
        @Override
        public void processElement(Integer integer, Context context) throws Exception {
            state.add(integer);
        }

        // 当请求检查点快照时调用次方法
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        // 在分布式执行期间创建并行函数实例时, 将调用次方法
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("state1", Types.INT));
        }
    }
}
