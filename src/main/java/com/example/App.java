package com.example;

import java.util.logging.LogManager;

public class App
{
    public static void main( String[] args )
    {
        if (args.length == 0) {
            LogManager.getLogManager().reset();
        }
        Config config = Config.getInstance();
        Orchestrator orchestrator = new Orchestrator(config);
        orchestrator.start();
    }
}
