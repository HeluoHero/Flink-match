import org.apache.commons.lang.time.DateFormatUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test2 {
    public static void main(String[] args) throws ParseException {
        String inputDate = "2024-04-12 10:30:45";
        String outputFormat = "yyyy-MM-dd HH:mm:ss.SSS";

        SimpleDateFormat inputFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat outputFormatter = new SimpleDateFormat(outputFormat);

        System.out.println(inputFormatter.parse(inputDate).getTime());

        System.out.println(outputFormatter.parse(DateFormatUtils.format(inputFormatter.parse(inputDate).getTime(), outputFormat)).getTime());

        /*try {
            Date date = inputFormatter.parse(inputDate);
            String outputDate = outputFormatter.format(date);
            System.out.println("Input Date: " + inputDate);
            System.out.println("Output Date: " + outputDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }*/
    }
}
