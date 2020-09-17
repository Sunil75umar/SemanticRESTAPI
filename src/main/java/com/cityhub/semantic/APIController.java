package com.cityhub.semantic;

import java.util.Arrays;
import java.util.LinkedList;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(ServerConfiguration.API_BASE_URI)
public class APIController {
	
	  /////////////////////////////////////////////
	 //------------>GET Graph List
	/////////////////////////////////////////////
	@RequestMapping(value = "/graphs")
	public String getGraphList(
								@RequestParam(value = "graphType") String[] graphType,
								@RequestParam(value = "keyword") String[] keywords,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam(value = "limit", defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		//-------------------->ontology, instance
		boolean graphTypes[] = { false, false };
		
		//-------------->Checking graphTypes
		for(int i=0; i<graphType.length && graphType!=null; i++) {
			
			if( graphType[i].equalsIgnoreCase("ontology") ){ graphTypes[0] = true; }
			if( graphType[i].equalsIgnoreCase("instance") ) { graphTypes[1] = true; }
		}
		
		return tdb_handler.execute_GraphSearchQuery(
													graphTypes,
													new LinkedList(Arrays.asList(keywords)),
													getIntegerFormatValue(prefixFormat),
													limit													
												);
	}
	
	
	
	
	  /////////////////////////////////////////////
	 //------------>GET Entity List
	/////////////////////////////////////////////	
	@RequestMapping(value = "/entities")
	public String getEntityList(
								@RequestParam(value = "keyword") String[] keywords,
								@RequestParam(value = "entityType") String[] entityTypes,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam(value = "limit", defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		return tdb_handler.execute_EntityListSearchQuery( 
														new LinkedList(Arrays.asList(keywords)),
														new LinkedList(Arrays.asList(entityTypes)),
														getIntegerFormatValue(prefixFormat),
														limit
														);
	}
	
	
	
	
	  /////////////////////////////////////////////
	 //---------->GET Individual List
	/////////////////////////////////////////////	
	@RequestMapping(value = "/individuals")
	public String getIndividualList(
								@RequestParam(value = "classId") String[] classIds,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam(value = "limit", defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		return tdb_handler.execute_IndividualSearchQuery( 
														new LinkedList(Arrays.asList(classIds)),
														getIntegerFormatValue(prefixFormat),
														limit
														);
	}
	
	
	
	  /////////////////////////////////////////////
	 //------------>GET Entity Info
	/////////////////////////////////////////////	
	@RequestMapping(value = "/entities/{entityURI}")
	public String getEntityInfo( 
								@PathVariable String entityURI, 
								@RequestParam (value = "responseType") String responseType,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam (value = "limit", defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		System.out.println("Retrieved Entity ID: " + entityURI);
		
		return tdb_handler.execute_GetEntityInfo( 
												entityURI, 
												getIntegerFormatValue(responseType), 
												getIntegerFormatValue(prefixFormat), 
												limit 
												);
		
	}
	
	
	
	
	  /////////////////////////////////////////////
	 //------------>GET Graph Info
	/////////////////////////////////////////////	
	@RequestMapping(value = "/graphs/{graphURI}")
	public String getGraphInfo( 
								@PathVariable String graphURI,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam (value = "limit", defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		return tdb_handler.execute_GetGraphInfo( graphURI, getIntegerFormatValue(prefixFormat), limit );
	}
	
	
	
	
	  /////////////////////////////////////////////
	 //----------->GET Class Hierarchy
	/////////////////////////////////////////////	
	@RequestMapping(value = "/classhierarchy/{classURI}")
	public String getClassHierarchy( 
								@PathVariable String classURI,
								@RequestParam(value = "prefixFormat", defaultValue = "normal") String prefixFormat,
								@RequestParam (value = "limit" , defaultValue = "1000") long limit
								) {
		
		TDBHandler tdb_handler = new TDBHandler();
		
		return tdb_handler.execute_GetClassHierarchy( classURI, getIntegerFormatValue(prefixFormat), limit );
	}
	
	
	
	
	
	
	int getIntegerFormatValue(String format) {
		
		int format_type = 0;
		
		if(format.equals("simple")) {
			
			format_type = 1;
			
		}else if(format.equals("normal")) {
			
			format_type = 2;
		}
		
		return format_type ;
	}
}
