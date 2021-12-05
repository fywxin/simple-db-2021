package io.iamazy.github.simpledb.systemtest;

import org.junit.Before;

import io.iamazy.github.simpledb.common.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
