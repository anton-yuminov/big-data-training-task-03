import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MapTest extends IpStats.Map {

    @Test
    public void extractIpAddress() throws Exception {
        String line = "ip78 - - [24/Apr/2011:08:27:16 -0400] \"GET /~techrat/vw_spotters/vw_412detail.jpg HTTP/1.1\" 200 81083 \"http://samochodyswiata.pl/viewtopic.php?f=14&t=34288\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.204 Safari/534.16\"";
        Text i = sut.extractIpAddress(line);
        assertEquals(i.toString(), "ip78");
    }

    private IpStats.Map sut = new IpStats.Map();

    @Test
    public void extractIpInfo() throws Exception {
        String line = "ip16 - - [24/Apr/2011:07:40:53 -0400] \"GET /sunFAQ/swapmyths.html HTTP/1.0\" 200 2063 \"-\" \"Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)\"";
        IpStats.IpInfo i = sut.extractIpInfo(line);
        assertEquals(i.totalBytes.get(), 2063L);
        assertEquals(i.requestsCount.get(), 1L);
    }

    @Test
    public void map() throws Exception {
        String s = "ip1586 - - [28/Apr/2011:00:23:43 -0400] \"GET /faq/sca_adapter.jpg HTTP/1.1\" 200 89442 \"http://host2/faq/\" \"Mozilla/5.0 (Windows NT 5.1; rv:2.0) Gecko/20100101 Firefox/4.0\"";
        IpStats.Map mapper = new IpStats.Map();
        MapDriver mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver
                .withInput(new LongWritable(1L), new Text(s))
                .withOutput(new Text("ip1586"), new IpStats.IpInfo(89442L, 1))
                .runTest();
    }

    @Test
    public void reduce() throws Exception {
        IpStats.Reduce reduce = new IpStats.Reduce();
        ReduceDriver reduceDriver = ReduceDriver.newReduceDriver(reduce);
        reduceDriver
                .withInput(new Text("ip1586"), new ArrayList<IpStats.IpInfo>() {{
                    add(new IpStats.IpInfo(10, 2));
                    add(new IpStats.IpInfo(5, 1));
                }})
                .withOutput(new Text("ip1586"), new IpStats.IpOutInfo(15L, 5f))
                .runTest();
    }

    @Test
    public void combine() throws Exception {
        IpStats.Combine combine = new IpStats.Combine();
        ReduceDriver reduceDriver = ReduceDriver.newReduceDriver(combine);
        reduceDriver
                .withInput(new Text("ip11"), new ArrayList<IpStats.IpInfo>() {{
                    add(new IpStats.IpInfo(10L, 1));
                    add(new IpStats.IpInfo(5L, 2));
                }})
                .withOutput(new Text("ip11"), new IpStats.IpInfo(15L, 3))
                .runTest();
    }
}