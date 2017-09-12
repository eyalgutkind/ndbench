package com.netflix.ndbench.plugin.cass;


import com.datastax.driver.core.*;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import org.slf4j.LoggerFactory;

import java.util.List;


@Singleton
@NdBenchClientPlugin("CassJavaDriverGeneric_uc_8col_txt")
@SuppressWarnings("unused")
public class CassJavaDriverGeneric_uc_8col_txt extends CJavaDriverBasePlugin {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(CassJavaDriverGeneric.class);


    @Inject
    public CassJavaDriverGeneric_uc_8col_txt(PropertyFactory propertyFactory, CassJavaDriverManager cassJavaDriverManager) {
        super(propertyFactory, cassJavaDriverManager);
    }

    @Override
    public String readSingle(String key) throws Exception {

        boolean success = true;
        int rowCount = 0;

        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("c0", key);
        bStmt.setConsistencyLevel(this.ReadConsistencyLevel);
        
        //ResultSetFuture future = session.executeAsync(bStmt);
        //ResultSet rs = future.get();
        
        ResultSet rs = session.execute(bStmt);
        
        List<Row> result=rs.all();

        if (!result.isEmpty())
        {
	    rowCount = result.size();
            if ( rowCount == 0 ) {
                Logger.warn("Key: "+key+". No rows returned.");
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    @Override
    public String writeSingle(String key) throws Exception {
        BoundStatement bStmt = writePstmt.bind();
        bStmt.setString("c0", key);
        bStmt.setString("c1", this.dataGenerator.getRandomValue());
        bStmt.setString("c2", this.dataGenerator.getRandomValue());
        bStmt.setString("c3", this.dataGenerator.getRandomValue());
        bStmt.setString("c4", this.dataGenerator.getRandomValue());
        bStmt.setString("c5", this.dataGenerator.getRandomValue());
        bStmt.setString("c6", this.dataGenerator.getRandomValue());
        bStmt.setString("c7", this.dataGenerator.getRandomValue());
        bStmt.setString("c8", this.dataGenerator.getRandomValue());
        
        bStmt.setConsistencyLevel(this.WriteConsistencyLevel);
        session.execute(bStmt);
        return ResultOK;
    }

    @Override
    void upsertKeyspace(Session session) {
       upsertGenereicKeyspace();
    }
    @Override
    void upsertCF(Session session) {
        session.execute("CREATE TABLE IF NOT EXISTS "+TableName+" ( c0 text, c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text, PRIMARY KEY ( c0 ) ) WITH COMPRESSION = {'sstable_compression': ''} AND speculative_retry = '4ms'");

    }

    @Override
    void preInit() {

    }

    @Override
    void postInit() {

    }


    @Override
    void prepStatements(Session session) {
        writePstmt = session.prepare("INSERT INTO "+TableName+" ( c0, c1, c2, c3, c4, c5, c6, c7, c8 ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )");
        readPstmt = session.prepare("SELECT * FROM "+TableName+" WHERE c0 = ?");
    }

}
