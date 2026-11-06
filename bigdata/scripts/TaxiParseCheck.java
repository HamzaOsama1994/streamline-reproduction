// Offline check that the authors' TAXI ETL front end accepts the converter's TAXI output (no cluster needed).
// Lines are read exactly as the authors' KafkaProducer reads them (OpenCSV, separator '|', skip header, field [1] is the
// Kafka value), then parsed with the authors' TaxiSenMLParserJSON.parseSenMLPack and passed through RangeFilterFunction
// (both from the ETL fat jar, unmodified).
// Run (JDK 11 single-file launch):
//   java -cp "kafkaProducer/build/KafkaProducer.jar;etl/build/FlinkJob.jar" scripts/TaxiParseCheck.java FILE N
import at.ac.uibk.dps.streamprocessingapplications.etl.taxi.RangeFilterFunction;
import at.ac.uibk.dps.streamprocessingapplications.shared.TaxiSenMLParserJSON;
import at.ac.uibk.dps.streamprocessingapplications.shared.model.TaxiRide;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.*;

public class TaxiParseCheck {
  public static void main(String[] a) throws Exception {
    long n = Long.parseLong(a[1]);
    CSVReader reader = new CSVReaderBuilder(new FileReader(a[0])).withSkipLines(1)
        .withCSVParser(new CSVParserBuilder().withSeparator('|').build()).build();
    RangeFilterFunction range = new RangeFilterFunction();
    String[] getters = {"getTaxiIdentifier", "getHackLicense", "getPickupDatetime", "getTripTimeInSecs", "getTripDistance",
        "getPickupLongitude", "getPickupLatitude", "getDropoffLongitude", "getDropoffLatitude", "getPaymentType",
        "getFareAmount", "getSurcharge", "getMtaTax", "getTipAmount", "getTollsAmount", "getTotalAmount"};
    Map<String, long[]> empty = new LinkedHashMap<>();  // [before range filter, after range filter]
    for (String g : getters) empty.put(g, new long[2]);
    long ok = 0, fail = 0, lines = 0;
    String firstValue = null, firstRide = null, firstError = null;
    String[] row;
    while (lines < n && (row = reader.readNext()) != null) {
      lines++;
      if (firstValue == null) firstValue = row[1];
      try {
        TaxiRide ride = TaxiSenMLParserJSON.parseSenMLPack(row[1]);
        for (int k = 0; k < 2; k++) {
          for (String g : getters) {
            Object v;
            try {
              Method m = TaxiRide.class.getMethod(g);
              v = m.invoke(ride);
            } catch (NoSuchMethodException e) {
              v = "n/a";
            }
            if (v == null || (v instanceof Optional && !((Optional<?>) v).isPresent())) empty.get(g)[k]++;
          }
          if (k == 0) ride = range.apply(ride);
        }
        if (firstRide == null) firstRide = ride.toString();
        ok++;
      } catch (Exception e) {
        fail++;
        if (firstError == null) firstError = e + " on: " + row[1].substring(0, Math.min(200, row[1].length()));
      }
    }
    System.out.println("lines " + lines + " parsed " + ok + " exceptions " + fail);
    System.out.println("first Kafka value starts with: [" + firstValue.substring(0, 80) + "]");
    System.out.println("first ride after range filter: " + firstRide);
    if (firstError != null) System.out.println("first error: " + firstError);
    System.out.println("empty fields (before range filter / after):");
    for (Map.Entry<String, long[]> e : empty.entrySet())
      System.out.println("  " + e.getKey() + " " + e.getValue()[0] + " / " + e.getValue()[1]);
  }
}
