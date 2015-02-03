package sheng;

import com.linkedin.rosetta.avro.Demo;
import com.linkedin.rosetta.avro.Firmo;
import com.linkedin.rosetta.avro.Granite;
import java.io.File;
import java.io.IOException;
import java.util.List;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


/**
 *
 * Created by shezhao on 1/16/15.
 */
public class WriteExcel {

  final static int ROW_PER_SHEET = 10000;

  public void writeBook(String outputFile, List<SpecificRecord> recordList)
      throws IOException, WriteException {
    WritableWorkbook workbook = Workbook.createWorkbook(new File(outputFile));
    WritableSheet sheet = null;



    Schema schema = null;
    int rowNum = 1;
    for (int i = 1; i <= recordList.size(); i++, rowNum++) {
      SpecificRecord specificRecord = recordList.get(i - 1);
      if (i == 1) {
        schema = specificRecord.getSchema();
      }
      if (i == 1 || (i % ROW_PER_SHEET == 0) ) {
        int sheetId = i / ROW_PER_SHEET;
        sheet = workbook.createSheet(sheetId + " Enrichment Task", sheetId);
        InitialSheet(sheet);
        rowNum = 1;
        System.out.println(sheetId + " Enrichment Task created" );
      }
      if (schema.getName().equals("Granite")) {
        Granite granite = (Granite) specificRecord;
        sheet.addCell(new Label(0, rowNum, getString(granite.getSourceRecordId())));
        sheet.addCell(new Label(1, rowNum, getString(granite.getSourceCode())));
        sheet.addCell(new Label(2, rowNum, getString(granite.getReferenceId())));
        sheet.addCell(new Label(3, rowNum, getString(granite.getName())));
        sheet.addCell(new Label(4, rowNum, getList(granite.getAka())));
        sheet.addCell(new Label(5, rowNum, getList(granite.getLogo())));
        sheet.addCell(new Label(6, rowNum, getString(granite.getDescription())));
        sheet.addCell(new Label(7, rowNum, getString(granite.getLocale())));
        Demo demo = granite.getDemographic();
        sheet.addCell(new Label(8, rowNum, getList(demo.getPhone())));
        sheet.addCell(new Label(9, rowNum, getList(demo.getFax())));
        sheet.addCell(new Label(10, rowNum, getList(demo.getUrl())));
        sheet.addCell(new Label(11, rowNum, getString(demo.getStreet())));
        sheet.addCell(new Label(12, rowNum, getString(demo.getCity())));
        sheet.addCell(new Label(13, rowNum, getString(demo.getState())));
        sheet.addCell(new Label(14, rowNum, getString(demo.getCountry())));
        sheet.addCell(new Label(15, rowNum, getString(demo.getPostalCode())));
        Firmo firmo = granite.getFirmographic();
        sheet.addCell(new Label(16, rowNum, getString(firmo.getOwnershipType())));
        sheet.addCell(new Label(17, rowNum, getString(firmo.getLocationType())));
        sheet.addCell(new Label(18, rowNum, getString(firmo.getCompanyStatus())));
        sheet.addCell(new Label(19, rowNum, getString(firmo.getEmployeeNum())));
        sheet.addCell(new Label(20, rowNum, getString(firmo.getYearFounded())));
        sheet.addCell(new Label(21, rowNum, getList(firmo.getIndustry())));
      }
    }

    workbook.write();
    workbook.close();
  }

  private void InitialSheet(WritableSheet sheet)
      throws WriteException {
    sheet.addCell(new Label(0, 0, "Source Record ID"));
    sheet.addCell(new Label(1, 0, "Source Code"));
    sheet.addCell(new Label(2, 0, "Reference ID"));
    sheet.addCell(new Label(3, 0, "Name"));
    sheet.addCell(new Label(4, 0, "AKA (Delimited by comma)"));
    sheet.addCell(new Label(5, 0, "Logo (Delimited by comma)"));
    sheet.addCell(new Label(6, 0, "Description"));
    sheet.addCell(new Label(7, 0, "Locale"));
    sheet.addCell(new Label(8, 0, "Phone (Delimited by comma)"));
    sheet.addCell(new Label(9, 0, "Fax (Delimited by comma)"));
    sheet.addCell(new Label(10, 0, "URL (Delimited by comma)"));
    sheet.addCell(new Label(11, 0, "Street"));
    sheet.addCell(new Label(12, 0, "City"));
    sheet.addCell(new Label(13, 0, "State"));
    sheet.addCell(new Label(14, 0, "Country"));
    sheet.addCell(new Label(15, 0, "Postal Code"));
    sheet.addCell(new Label(16, 0, "Ownership Type"));
    sheet.addCell(new Label(17, 0, "Location Type"));
    sheet.addCell(new Label(18, 0, "Company Status"));
    sheet.addCell(new Label(19, 0, "Employee Num"));
    sheet.addCell(new Label(20, 0, "Year Funded"));
    sheet.addCell(new Label(21, 0, "Industry (Delimited by comma)"));
  }

  private String getString(String in) {
    return (in == null) ? "" : in;
  }

  private String getList(List<String> stringList) {
    return (stringList == null || stringList.size() == 0) ? "" : stringList.toString().replaceAll("\\[\\]", "");
  }

  public static void main(String[] args)
      throws IOException, WriteException, InstantiationException, IllegalAccessException {
    WriteExcel writeExcel = new WriteExcel();
    ReadAvro readAvro = new ReadAvro();
    writeExcel.writeBook("/tmp/sample.xls", readAvro.getAvroList("/Users/shezhao/Scratch/enrichD_ToBeEnriched-m-00001.avro", Granite
        .getClassSchema()));
  }
}
