package com.asiainfo.ctc.eda.UDAF;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum.GenericUDAFSumDouble;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

public class GenericUDAFSum extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(GenericUDAFSum.class.getName());
 
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
		}

		if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
		}
		if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
					"Only primitive type arguments are accepted but " + parameters[1].getTypeName() + " is passed.");
		}
		switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
		case BYTE:
		case SHORT:
		case INT:
		case LONG:
		case TIMESTAMP:
			return new GenericUDAFSumLong();
		case FLOAT:
		case DOUBLE:
		case STRING:
			return new GenericUDAFSumDouble();
		case BOOLEAN:
		default:
			throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but "
					+ parameters[0].getTypeName() + " is passed.");
		}
	}

	public static class GenericUDAFSumLong extends GenericUDAFEvaluator {

		private PrimitiveObjectInspector inputOI;
		private LongWritable result;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			assert (parameters.length == 1);
			super.init(m, parameters);
			result = new LongWritable(0);
			inputOI = (PrimitiveObjectInspector) parameters[0];
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}

		/** 存储sum的值的类 */
		@SuppressWarnings("deprecation")
		static class SumLongAgg implements AggregationBuffer {
			boolean empty;
			long sum;
		}

		// 创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。

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

		// map阶段调用，只要把保存当前和的对象agg，再加上输入的参数，就可以了。
		@SuppressWarnings("deprecation")
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			assert (parameters.length == 1);
			try {
				merge(agg, parameters[0]);
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

		// combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
		@SuppressWarnings("deprecation")
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				SumLongAgg myagg = (SumLongAgg) agg;
				myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
				myagg.empty = false;
			}
		}

		// reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
		@SuppressWarnings("deprecation")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SumLongAgg myagg = (SumLongAgg) agg;
			if (myagg.empty) {
				return null;
			}
			result.set(myagg.sum);
			return result;
		}

	}

	/*public static enum Mode {
		*//**
		 * PARTIAL1: 这个是mapreduce的map阶段:从原始数据到部分数据聚合
		 * 将会调用iterate()和terminatePartial()
		 *//*
		PARTIAL1,
		*//**
		 * PARTIAL2:
		 * 这个是mapreduce的map端的Combiner阶段，负责在map端合并map的数据::从部分数据聚合到部分数据聚合:
		 * 将会调用merge() 和 terminatePartial()
		 *//*
		PARTIAL2,
		*//**
		 * FINAL: mapreduce的reduce阶段:从部分数据的聚合到完全聚合 将会调用merge()和terminate()
		 *//*
		FINAL,
		*//**
		 * COMPLETE:
		 * 如果出现了这个阶段，表示mapreduce只有map，没有reduce，所以map端就直接出结果了:从原始数据直接到完全聚合 将会调用
		 * iterate()和terminate()
		 *//*
		COMPLETE
	}*/
}
