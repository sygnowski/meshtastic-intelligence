package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import io.github.s7i.meshtastic.intelligence.nodeinfo.MapNodeInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
public class MeshNodeInfoJob extends JobStub {

    TableEnvironment tableEnv;

    public MeshNodeInfoJob(ParameterTool params, StreamExecutionEnvironment env,
          Configuration cfg) {
        super(params, env, cfg);

        tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance()
              .withBuiltInCatalogName("mesh-catalog")
              .withBuiltInDatabaseName("mesh-db")
              .inStreamingMode()
              .build());
    }

    void initCatalog() {
        var catalogName = cfg.getOption("catalog.name");
        var databaseName = cfg.getOption("database.name");
        var baseUrl = cfg.getOption("catalog.jdbc");
        var username = cfg.getOption("user.name");
        var password = cfg.getOption("user.password");

        //var loader = IntelligenceJob.class.getClassLoader();
        var catalog = new JdbcCatalog(catalogName, databaseName, username, password, baseUrl);

        log.debug("Catalog :: List databases: {}", catalog.listDatabases());

        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        //tableEnv.useDatabase(databaseName);

        log.debug("catalogs: {}", (Object) tableEnv.listCatalogs());

        cfg.onTrue("user.catalog.sql.create",
              () -> tableEnv.executeSql(cfg.getOption("sql.create.mesh.table")));
    }

    @Override
    public void build() {
        //initCatalog();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        var nodeInfoStream = fromKafkaSource()
              .disableChaining()
              .filter(packet -> FromRadio.parseFrom(packet.payload())
                    .getPayloadVariantCase() == PayloadVariantCase.NODE_INFO)
              .name("Node Info Filter")
              .map(new MapNodeInfo())
              .returns(Types.ROW_NAMED(new String[]{
                    "node_id", "user_id", "user_name", "user_long_name", "user_role", "last_heard", "hops_away", "snr"
              }, Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME, Types.INT, Types.FLOAT))
              .name("Map Node Info")
              .disableChaining();

        var tab = streamTableEnv.fromDataStream(nodeInfoStream, Schema.newBuilder()
              .column("node_id", DataTypes.BIGINT())
              .column("user_id", DataTypes.STRING())
              .column("user_name", DataTypes.STRING())
              .column("user_long_name", DataTypes.STRING())
              .column("user_role", DataTypes.STRING())
              .column("last_heard", DataTypes.TIMESTAMP())
              .column("hops_away", DataTypes.INT())
              .column("snr", DataTypes.FLOAT())
              .build());

        tab.printSchema();

        var resultStream = streamTableEnv.toChangelogStream(tab);
        resultStream.print();

        cfg.findOption("sql.create.nodeinfo.table").ifPresent(sql -> {
            streamTableEnv.createTemporaryView("NodeInfo", tab);
            streamTableEnv.executeSql(sql);
            streamTableEnv.executeSql("INSERT INTO node_info_sink SELECT * FROM NodeInfo");
        });
    }
}
