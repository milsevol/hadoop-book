// == SingleResourceConfigurationTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SingleResourceConfigurationTest {
  
  @Test
  public void get() throws IOException {
    // vv SingleResourceConfigurationTest
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    System.out.println(conf.get("color"));
    assertThat(conf.get("color"), is("yellow"));
    assertThat(conf.getInt("size", 0), is(10));
    // get()方法允许读取xml中没有的值，如这个breadth,在这里指定了默认值为wide
    assertThat(conf.get("breadth", "wide"), is("wide"));
    // ^^ SingleResourceConfigurationTest
  }

}
