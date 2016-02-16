package com.asiainfo.ctc.eda.UDAF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

//fail to run the except result
public class DDRMerge extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(DDRMerge.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		//参数个数检查
		if (parameters.length != 3) {
			throw new UDFArgumentTypeException(parameters.length , "Exactly three argument is expected.");
		}
		//参数类型检查
		/*if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
		}
		if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but " + parameters[1].getTypeName() + " is passed.");
		}*/
		//这一步在该程序没有作用，一般用于参数类型的重载，详见org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum
		/*switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
		case BYTE:
		case SHORT:
		case INT:
		case LONG:
			return new GenericUDAFSumLong();
		default:
			throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but "
					+ parameters[0].getTypeName() + " is passed.");
		}*/
		
		return new GenericUDAFSumLong();
	}

	//18500394811	2012026143	2012026188	108
	//参数：        (start,      endtime,   flow)
	public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector inputOI0;
		private PrimitiveObjectInspector inputOI1;
		private PrimitiveObjectInspector inputOI2;
		//用于接收输出结果
		private LongWritable result;
		//用于接收行纪录求和
//		private LongWritable rowsum;

		//初始化方法，在Mode的每一个阶段启动时会执行init方法
		//每个阶段parameters数组的长度都可能不一致
		@Override
		public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
			int length = parameters.length;
			
			/*System.out.println(mode);
			System.out.println(parameters.length);
			System.out.println(parameters[0]);
			System.out.println(parameters[1]);
			System.out.println("----------------");*/
			
			super.init(mode, parameters);
			result = new LongWritable(0);
			
			if (length > 1) {
				assert (mode == Mode.COMPLETE || mode == Mode.PARTIAL1);
				initMapSide(parameters);
			}else {
				assert (mode == Mode.FINAL || mode == Mode.PARTIAL2);
				assert (parameters.length == 1);
				initReduceSide(parameters[0]);
			}
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}

		private void initMapSide(ObjectInspector[] parameters) {
			inputOI0 = (PrimitiveObjectInspector) parameters[0];
			inputOI1 = (PrimitiveObjectInspector) parameters[1];
			inputOI2 = (PrimitiveObjectInspector) parameters[2];
		}
		
		private void initReduceSide(ObjectInspector objectInspector) {
			
		}

		//用于存放分组后纪录间的求和临时值
		/** class for storing double sum value. */
		@AggregationType(estimable = true)
		static class SumLongAgg extends AbstractAggregationBuffer {
			int start;
			int endtime;
			long sum;
			boolean empty = true;

			@Override
			public int estimate() {
				return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2;
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			SumLongAgg result = new SumLongAgg();
			reset(result);
			return result;
		}

		@SuppressWarnings("deprecation")
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			SumLongAgg myagg = (SumLongAgg) agg;
			myagg.empty = true;
			myagg.sum = 0;
		}

		private boolean warned = false;

		//map阶段时两列相加
		@SuppressWarnings("deprecation")
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 3);
			try {
				int num1 = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI0);
				int num2 = PrimitiveObjectInspectorUtils.getInt(parameters[1], inputOI1);
				long flow = PrimitiveObjectInspectorUtils.getLong(parameters[2], inputOI2);
				
//				StringObjectInspector strInputOI;
//				inputOI0 = (PrimitiveObjectInspector) parameters[1];
//				int id = PrimitiveObjectInspectorUtils.getInt(parameters[1], inputOI0);
//				if ("male".equals(gend) || "female".equals(gend)) {
//				if (id == 1) {
//				if ("male".equals(gend)) {
				// if (true) {
//					merge(agg, rowsum);
//				}
				SumLongAgg myagg = (SumLongAgg)agg;
				if (myagg.empty) {
					myagg.start = num1;
					myagg.endtime = num2;
					System.out.println("the first record to be iterate");
				}else if (num1>=myagg.start && num1<=myagg.endtime){
					merge(myagg, flow);
					System.out.println("the record fit in the restriction");
				}/*else {
					terminatePartial(myagg);
					System.out.println("the record don't fit in");
				}*/
			} catch (NumberFormatException e) {
				if (!warned) {
					warned = true;
					LOG.warn(getClass().getSimpleName() + " " + StringUtils.stringifyException(e));
				}
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return terminate(agg);
		}

		//列和合并
		@SuppressWarnings("deprecation")
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				SumLongAgg myagg = (SumLongAgg) agg;
//				myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI0);
				myagg.sum += ((LongWritable)partial).get();
				myagg.empty = false;
			}
		}

		@SuppressWarnings("deprecation")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SumLongAgg myagg = (SumLongAgg) agg;
			if (myagg.empty) {
				return new LongWritable(1);
			}
			result.set(myagg.sum);
			return result;
		}
	}
}