package sheng;

import com.linkedin.rosetta.avro.Granite;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;


/**
 * Created by shezhao on 1/16/15.
 */
public class ReadAvro {

  public List<SpecificRecord> getAvroList(String fileName, Schema schema)
      throws IOException, IllegalAccessException, InstantiationException {
    SpecificDatumReader specificDatumReader = new SpecificDatumReader(schema);
    DataFileReader dataFileReader = new DataFileReader(new File(fileName), specificDatumReader);
    List<SpecificRecord> list = new ArrayList<SpecificRecord>();
    while (dataFileReader.hasNext()) {
      list.add((SpecificRecord) dataFileReader.next());
    }
    return list;
  }

  public static void main(String[] args)
      throws IllegalAccessException, IOException, InstantiationException {
    ReadAvro readAvro = new ReadAvro();
    List<SpecificRecord> avroList =
        readAvro.getAvroList("/Users/shezhao/Scratch/enrichD_ToBeEnriched-m-00001.avro", Granite.getClassSchema());
    System.out.println(avroList.size());
  }
}
