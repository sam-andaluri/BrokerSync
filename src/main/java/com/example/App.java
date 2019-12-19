package com.example;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

public class App 
{
    private static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args )
    {
        boolean isPrimary = true;
        String configFileName;
        Orchestrator primaryProcessor;

        OptionParser parser = new OptionParser();
        parser.accepts("m", "mode").withRequiredArg().ofType(String.class);
        parser.accepts("c", "config file location").withOptionalArg().ofType(String.class);
        OptionSet options = parser.parse( args );
        if (options.has("m") && options.valueOf("m").equals("standby")) isPrimary = false;
        if (options.has("c") && options.hasArgument("c")) {
            configFileName = (String)options.valueOf("c");
        } else {
            if (isPrimary) {
                configFileName = "primary.yaml";
            } else {
                configFileName = "standby.yaml";
            }
        }

        Constructor constructor = new Constructor(Config.class);
        Yaml yaml = new Yaml( constructor );
        InputStream inputStream = App.class
                .getClassLoader()
                .getResourceAsStream(configFileName);
        Config config = yaml.loadAs(inputStream, Config.class);
        logger.info("main " + yaml.dump(config));
        if (isPrimary) {
            primaryProcessor = new Orchestrator(config);
            primaryProcessor.start();
        }
    }
}
