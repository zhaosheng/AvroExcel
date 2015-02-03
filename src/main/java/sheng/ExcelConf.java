package sheng;

/**
 *
 * Created by shezhao on 2/2/15.
 */
public class ExcelConf {
  private int _columnId;
  private String _columnName;

  public ExcelConf(int columnId, String columnName) {
    _columnId = columnId;
    _columnName = columnName;
  }

  public int getColumnId() {
    return _columnId;
  }

  public void setColumnId(int columnId) {
    _columnId = columnId;
  }

  public String getColumnName() {
    return _columnName;
  }

  public void setColumnName(String columnName) {
    _columnName = columnName;
  }
}
