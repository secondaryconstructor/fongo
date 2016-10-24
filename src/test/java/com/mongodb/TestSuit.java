package com.mongodb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.github.fakemongo.FongoAggregateGroupTest;
import com.github.fakemongo.FongoAggregateOutTest;
import com.github.fakemongo.FongoAggregateProjectTest;
import com.github.fakemongo.FongoAggregateTest;
import com.github.fakemongo.FongoFindTest;
import com.github.fakemongo.FongoGeoTest;
import com.github.fakemongo.FongoGridFSTest;
import com.github.fakemongo.FongoIndexTest;
import com.github.fakemongo.FongoMapReduceOutputModesTest;
import com.github.fakemongo.FongoMapReduceTest;
import com.github.fakemongo.FongoTest;
import com.github.fakemongo.FongoTextSearchTest;
import com.github.fakemongo.JongoGeoTest;
import com.github.fakemongo.LookupTest;
import com.github.fakemongo.impl.ExpressionParserTest;
import com.github.fakemongo.impl.UpdateEngineTest;
import com.github.fakemongo.impl.geo.GeoUtilTest;
import com.github.fakemongo.impl.index.IndexTest;
import com.github.fakemongo.impl.index.IndexedListTest;
import com.github.fakemongo.integration.SpringFongoTest;
import com.github.fakemongo.integration.SpringMongoOperationTest;
import com.github.fakemongo.integration.SpringQueryTest;
import com.github.fakemongo.integration.jongo.FongoJongoTest;


/**
 * 
 * @author Nicola Viola
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	GeoUtilTest.class,
	IndexedListTest.class,
	IndexTest.class,
	ExpressionParserTest.class,
	UpdateEngineTest.class,
	FongoJongoTest.class,
	SpringFongoTest.class,
	SpringMongoOperationTest.class,
	SpringQueryTest.class,
	FongoAggregateGroupTest.class,
	FongoAggregateOutTest.class,
	FongoAggregateProjectTest.class,
	FongoAggregateTest.class,
	//FongoAggregateZipTest.class, //this test doesn't finish
	FongoFindTest.class,
	FongoGeoTest.class,
	FongoGridFSTest.class,
	FongoIndexTest.class,
	FongoMapReduceOutputModesTest.class,
	FongoMapReduceTest.class,
	FongoTest.class,
	FongoTextSearchTest.class,
	JongoGeoTest.class,
	LookupTest.class,
	
	FongoCastTest.class,
	FongoDBCollectionTest.class,
	FongoDBTest.class,
	FongoMongoDatabaseTest.class,
	FongoMongoTest.class,
	
	CollectionNameTest.class
	
})

public class TestSuit {

	

}
