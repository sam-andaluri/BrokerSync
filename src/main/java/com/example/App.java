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
        String configFileName = null;
        Orchestrator orchestrator;

        OptionParser parser = new OptionParser();
        parser.accepts("c", "config file location").withOptionalArg().ofType(String.class);
        OptionSet options = parser.parse( args );
        if (options.has("c") && options.hasArgument("c")) {
            configFileName = (String)options.valueOf("c");
        }

        Constructor constructor = new Constructor(Config.class);
        Yaml yaml = new Yaml( constructor );
        InputStream inputStream = App.class
                .getClassLoader()
                .getResourceAsStream(configFileName);
        Config config = yaml.loadAs(inputStream, Config.class);
        logger.info("main " + yaml.dump(config));

        orchestrator = new Orchestrator(config);
        orchestrator.start();
    }
}
