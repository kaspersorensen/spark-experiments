package dk.sorensen.kasper;

import java.io.File;

import junit.framework.TestCase;

public class MySparkAppTest extends TestCase {

    public void testRunSomething() throws Exception {
        final String outputPath = "target/output-" + System.currentTimeMillis() + ".txt";
        final File outputFile = new File(outputPath);
        
        
        final MySparkApp app = new MySparkApp();
        app.run("src/test/resources/input1.txt", null, "local");

        assertTrue(outputFile.exists());
    }
}
