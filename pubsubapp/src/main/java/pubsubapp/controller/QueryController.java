package pubsubapp.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import pubsubapp.service.QueryService;
import pubsubapp.vo.Query;

@RestController
@RequestMapping("/query")
public class QueryController {

	@Autowired
	private QueryService queryService;

	@RequestMapping(value = "/add", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
	public String execute(@RequestBody Query query) throws Exception {
		System.out.println("Received "+query);
		return queryService.publishQuery(query);
	}
	
	@RequestMapping(value="/subscribe")
	public List<String> subscribeQueryOutput() throws FileNotFoundException, IOException {
		return queryService.subscribeQueryOutput();
	}
	
}
