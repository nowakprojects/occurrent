package org.occurrent.eventstore.sql.spring.reactor;

import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

import java.util.List;

import static java.util.Objects.requireNonNull;

class FilterConverter {

  //TODO: What about time representation and filedNamePrefix?
  public static String convertFilterToWhereClause(Filter filter) {
    String fieldNamePrefix = null;
    requireNonNull(filter, "Filter cannot be null");

    return convertFilterToSql(filter, fieldNamePrefix);
  }

  private static String convertFilterToSql(Filter filter, String fieldNamePrefix) {
    if (filter instanceof Filter.All) {
      return "";
    } else if (filter instanceof Filter.SingleConditionFilter) {
      Filter.SingleConditionFilter scf = (Filter.SingleConditionFilter) filter;
      String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName);
      Condition<?> conditionToUse = scf.condition;
      return convertConditionToCriteria(fieldName, conditionToUse);
    }
    return "";
  }

  private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
    return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
  }

  public static <T> String convertConditionToCriteria(String fieldName, Condition<T> condition) {
    if (condition instanceof Condition.MultiOperandCondition) {
      Condition.MultiOperandCondition<T> operation = (Condition.MultiOperandCondition<T>) condition;
      Condition.MultiOperandConditionName operationName = operation.operationName;
      List<Condition<T>> operations = operation.operations;
      String criteria = operations.stream().map(c -> convertConditionToCriteria(fieldName, c)).reduce("", (s1, s2) -> s1 + " " + s2);
      switch (operationName) {
        case AND:
          return " AND " + criteria;
        case OR:
          return " OR " + criteria;
        case NOT:
          return " NOT " + criteria;
        default:
          throw new IllegalStateException("Unexpected value: " + operationName);
      }
    } else if (condition instanceof Condition.SingleOperandCondition) {
      Condition.SingleOperandCondition<T> singleOperandCondition = (Condition.SingleOperandCondition<T>) condition;
      T value = singleOperandCondition.operand;
      Condition.SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
      switch (singleOperandConditionName) {
        case EQ:
          return fieldName + " = " + value;
        case LT:
          return fieldName + " < " + value;
        case GT:
          return fieldName + " > " + value;
        case LTE:
          return fieldName + " <= " + value;
        case GTE:
          return fieldName + " >= " + value;
        case NE:
          return fieldName + " != " + value;
        default:
          throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
      }
    } else {
      throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
    }
  }
}
