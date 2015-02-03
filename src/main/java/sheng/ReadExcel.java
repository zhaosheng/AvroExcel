package sheng;

import com.linkedin.rosetta.avro.Magma;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


/**
 * Created by shezhao on 2/2/15.
 */
public class ReadExcel {
  public List<ExcelConf> readExcelConf(String confPath)
      throws IOException {
    ArrayList<ExcelConf> excelConfs = new ArrayList<ExcelConf>();

    BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(confPath)));
    try {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] split = line.split(",");
        if (split.length != 2) {
          continue;
        }
        excelConfs.add(new ExcelConf(Integer.valueOf(split[0]), split[1]));
      }
    } finally {
      bufferedReader.close();
    }

    return excelConfs;
  }

  public void read(List<ExcelConf> excelConf, String excelFilePath, String avroFilePath, Schema schema) {
    if (excelConf == null) {
      return;
    }


    SpecificDatumWriter<SpecificRecord> specificDatumWriter = new SpecificDatumWriter<SpecificRecord>(schema);
    DataFileWriter<SpecificRecord> dataFileWriter = new DataFileWriter<SpecificRecord>(specificDatumWriter);
    File avroFile = new File(avroFilePath);
    try {
      Workbook workbook = Workbook.getWorkbook(new File(excelFilePath));
      Sheet sheet = workbook.getSheet(0);
      dataFileWriter.create(schema, avroFile);
      SpecificRecord record = (SpecificRecord) Class.forName(schema.getFullName()).newInstance();
      record.put(record.getSchema().getField("source_code").pos(), "BLR Rosetta");
      record.put(record.getSchema().getField("created_ts").pos(), String.valueOf(System.currentTimeMillis()));
      try {
        for (int i = 1; i < sheet.getRows(); i++) {
          for (ExcelConf conf : excelConf) {
            Cell cell = sheet.getCell(conf.getColumnId(), i);
            setRecord(record, conf.getColumnName(), cell.getContents());
          }
          dataFileWriter.append(record);
        }
      } finally {
        dataFileWriter.close();
        workbook.close();
      }
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (InvocationTargetException e1) {
      e1.printStackTrace();
    } catch (ClassNotFoundException e1) {
      e1.printStackTrace();
    } catch (BiffException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("unchecked")
  private void setRecord(SpecificRecord record, String fieldName, String fieldValue)
      throws InvocationTargetException, IllegalAccessException, ClassNotFoundException, InstantiationException {
    Schema schema = record.getSchema();
    Method[] methods = record.getClass().getMethods();
    String[] split = fieldName.split("\\.", -1);
    String c = split[0];
    Method getMethod = getMethod(methods, capitalMethodName(c));
    Method setMethod = setMethod(methods, capitalMethodName(c));
    if (split.length > 1) {
      Object invoke = getMethod.invoke(record);
      SpecificRecord innerRecord;
      Schema elementSchema = null;
      Schema containerSchema = schema.getField(c).schema();
      List<Schema> types = containerSchema.getTypes();
      for (Schema type : types) {
        if (type.getType() == Schema.Type.NULL) {
          continue;
        } else if (type.getType() == Schema.Type.ARRAY  && type.getElementType().getType() == Schema.Type.RECORD) {
          containerSchema = type;
          elementSchema = type.getElementType();
        } else if (type.getType() == Schema.Type.RECORD) {
          containerSchema = type;
          elementSchema = type;
        }
      }
      if (invoke == null) {
        // make object
        assert elementSchema != null;
        innerRecord = (SpecificRecord) Class.forName(elementSchema.getFullName()).newInstance();
      } else {
        innerRecord = null;
        if (containerSchema.getType() == Schema.Type.ARRAY) {
          List<SpecificRecord> list = (List<SpecificRecord>) invoke;
          innerRecord = list.get(0);
        } else if (containerSchema.getType() == Schema.Type.RECORD) {
          innerRecord = (SpecificRecord) invoke;
        }
      }
      setRecord(innerRecord, fieldName.substring(fieldName.indexOf(".") + 1), fieldValue);
      if (containerSchema.getType() == Schema.Type.ARRAY) {
        List<SpecificRecord> innerRecordList = new LinkedList<SpecificRecord>();
        innerRecordList.add(innerRecord);
        setMethod.invoke(record, innerRecordList);
      } else if (containerSchema.getType() == Schema.Type.RECORD) {
        setMethod.invoke(record, innerRecord);
      }
    } else {
      String[] split1 = c.split("#", -1);
      if (split1.length > 1) {
        // Map key
        Object invoke = getMethod.invoke(record);
        if (invoke == null) {
          setMethod.invoke(record, new HashMap<String, String>());
        }
        HashMap<String, String> mapObj = (HashMap<String, String>) getMethod.invoke(record);
        mapObj.put(split1[1], fieldValue);

      } else {
        Type type = getMethod.getAnnotatedReturnType().getType();
        if (type.getTypeName().equals("java.util.List<java.lang.String>")) {
          List<String> list = new LinkedList<String>();
          if (fieldValue != null && fieldValue.trim().length() > 0)
            list.add(fieldValue);
          setMethod.invoke(record, list);
        } else if (type.getTypeName().equals("java.lang.String")) {
          setMethod.invoke(record, fieldValue);
        }
      }
    }
  }

  private Method getMethod(Method[] methods, String name) {
    for (Method method : methods) {
      if (method.getName().equalsIgnoreCase("get" + name.replaceAll("#.*", ""))) {
        return method;
      }
    }
    return null;
  }

  private Method setMethod(Method[] methods, String name) {
    for (Method method : methods) {
      if (method.getName().equalsIgnoreCase("set" + name.replaceAll("#.*", ""))) {
        return method;
      }
    }
    return null;
  }

  private String capitalMethodName(String name) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      if (name.charAt(i) == '_') {
        stringBuilder.append(String.valueOf(name.charAt(++i)).toUpperCase());
      } else {
        stringBuilder.append(name.charAt(i));
      }
    }
    return stringBuilder.toString();
  }

  public static void main(String[] args)
      throws IOException {
    ReadExcel readExcel = new ReadExcel();
    readExcel.read(readExcel.readExcelConf("src/main/resources/BLR_Rosetta_Enrichment_Result_10K_12_22_14.conf"), "/Users/shezhao/Rosetta-enrichment/BLR Rosetta Enrichment Result 10k 12_22_14 Completed.xls", "/Users/shezhao/Rosetta-enrichment/BLR_Rosetta_Enrichment_Result_10K_12_22_14.avro", Magma.getClassSchema());
    readExcel.read(readExcel.readExcelConf("src/main/resources/MNL_Rosetta_Enrichment_Result_10k.conf"), "/Users/shezhao/Rosetta-enrichment/MNL Rosetta Enrichment Result 10K.xls", "/Users/shezhao/Rosetta-enrichment/MNL_Rosetta_Enrichment_Result_10K.avro", Magma.getClassSchema());
  }
}
