package storm;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by affo on 1/11/16.
 */
public class TaxiRide implements Serializable {
    private static DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 2013-01-01 00:02:00
    public Date pickupTS, dropoffTS;
    public String pickupCell, dropoffCell, taxiID, license;
    public double fare, tip;

    public static TaxiRide parse(String line) throws AreaMapper.OutOfGridException, ParseException {
        TaxiRide tr = new TaxiRide();

        String[] tokens = line.split(",");

        double puLat = Double.valueOf(tokens[7]);
        double puLong = Double.valueOf(tokens[6]);
        double doLat = Double.valueOf(tokens[9]);
        double doLong = Double.valueOf(tokens[8]);
        tr.pickupCell = AreaMapper.getCellID(puLat, puLong);
        tr.dropoffCell = AreaMapper.getCellID(doLat, doLong);

        tr.taxiID = tokens[0];
        tr.license = tokens[1];
        tr.pickupTS = dateFmt.parse(tokens[2]);
        tr.dropoffTS = dateFmt.parse(tokens[3]);
        tr.fare = Double.valueOf(tokens[11]);
        tr.tip = Double.valueOf(tokens[14]);

        return tr;
    }
}
