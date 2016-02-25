package com.asiainfo.ctc.eda.UDAF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
 
/**
 * UDAF--练手程序
 * 该程序是用于分组后的两列总和再求和
 * eg:	kinyi   98      80
 *		kinyi   90      89
 *		kinyi   92      90
 *		allen   70      97
 *		allen   86      88 
 *		allen   99      97
 * --select name,rowsum(math) from tworowsum group by name;--
 * 		allen	537
 *		kinyi	539
 * 
 * @author Kinyi_Chan
 *
 */
public class TwoRowsSum extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(GenericUDAFSum.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		//参数个数检查
		if (parameters.length != 2) {
			throw new UDFArgumentTypeException(parameters.length , "Exactly two argument is expected.");
		}
		//参数类型检查
		if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
		}
		if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but " + parameters[1].getTypeName() + " is passed.");
		}
		//这一步在该程序没有作用，一般用于参数类型的重载，详见org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum
		switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
		case BYTE:
		case SHORT:
		case INT:
		case LONG:
			return new GenericUDAFSumLong();
		default:
			throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but "
					+ parameters[0].getTypeName() + " is passed.");
		}
	}

	/**
	 *  init方法没有分参数长度时
	 *  报错信息：
	 *  PARTIAL1
	 *  2
	 *	org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector@2f5287
	 *	org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector@2f5287
	 *	----------------
	 *	FINAL
	 *	1
	 *	org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector@5cff590c
	 *	FAILED: ArrayIndexOutOfBoundsException 1
	 *
	 */
	public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector inputOI0;
		private PrimitiveObjectInspector inputOI1;
		//用于接收输出结果
		private LongWritable result;
		//用于接收行纪录求和
		private LongWritable rowsum;

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
		}
		
		private void initReduceSide(ObjectInspector objectInspector) {
			
		}

		//用于存放分组后纪录间的求和临时值
		/** class for storing double sum value. */
		@AggregationType(estimable = true)
		static class SumLongAgg extends AbstractAggregationBuffer {
			boolean empty;
			long sum;

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
			assert (parameters.length == 2);
			try {
				
				rowsum = new LongWritable(0);
				long num1 = PrimitiveObjectInspectorUtils.getLong(parameters[0], inputOI0);
				long num2 = PrimitiveObjectInspectorUtils.getLong(parameters[1], inputOI1);
				
				rowsum.set(num1 + num2);
				
//				StringObjectInspector strInputOI;
//				inputOI0 = (PrimitiveObjectInspector) parameters[1];
//				int id = PrimitiveObjectInspectorUtils.getInt(parameters[1], inputOI0);
//				if ("male".equals(gend) || "female".equals(gend)) {
//				if (id == 1) {
//				if ("male".equals(gend)) {
				// if (true) {
					merge(agg, rowsum);
//				}
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