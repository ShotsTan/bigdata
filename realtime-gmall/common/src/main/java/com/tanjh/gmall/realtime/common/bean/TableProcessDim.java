package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: TableProcessDim
 * @Description: TODO 类描述
 * @Author: Tanjh
 * @Date: 2024/03/12 14:21
 * @Company: Copyright©
 **/


@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkRowKey;

    // 配置表操作类型
    String op;

}
