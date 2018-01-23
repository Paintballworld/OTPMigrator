package kz.greetgo;

import kz.greetgo.migration.Migrator;
import kz.greetgo.parameters.Parameters;

public class Main {

  public static void main(String[] args) throws Exception {

    Parameters.prepareArguments(args);
    Parameters.resolveArguments();

    Migrator migrator = new Migrator();
    migrator.mainMigrationProcess();

  }

}
