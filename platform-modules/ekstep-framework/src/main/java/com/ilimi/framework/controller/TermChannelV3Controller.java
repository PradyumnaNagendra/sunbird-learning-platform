package com.ilimi.framework.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ilimi.common.controller.BaseController;
import com.ilimi.common.dto.Request;
import com.ilimi.common.dto.Response;
import com.ilimi.common.exception.ClientException;
import com.ilimi.common.logger.PlatformLogger;
import com.ilimi.framework.enums.TermEnum;
import com.ilimi.framework.mgr.ITermManager;

/**
 * @author pradyumna
 *
 */
@Controller
@RequestMapping("/v3/channel/term")
public class TermChannelV3Controller extends BaseController {

	@Autowired
	private ITermManager termManager;

	private String graphId = "domain";

	/**
	 * 
	 * @param categoryId
	 * @param channelId
	 * @param requestMap
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/create", method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<Response> create(@RequestParam(value = "category", required = true) String categoryId, @RequestHeader(value = "X-Channel-Id") String channelId, @RequestBody Map<String, Object> requestMap) {
		String apiId = "ekstep.learning.channel.term.create";
		Request request = getRequest(requestMap);
		try {
			Map<String, Object> map = (Map<String, Object>) request.get("term");
			String termLabel=(String)map.get(TermEnum.label.name());
			if (termManager.validateRequest(channelId, categoryId)) {
				if(termManager.validateMasterTerm(categoryId, termLabel)){
					Response response = termManager.createTerm(categoryId, map);
					return getResponseEntity(response, apiId, null);
				}else{
					throw new ClientException("ERR_INVALID_TERM_ID", "Invalid Term Id.", apiId, null);
				}
				
			} else {
				throw new ClientException("ERR_INVALID_CATEGORY_ID", "Invalid CategoryId for Term", apiId, null);
			}
		} catch (Exception e) {
			PlatformLogger.log("create term", e.getMessage(), e);
			return getExceptionResponseEntity(e, apiId, null);
		}
	}

	/**
	 * 
	 * @param termId
	 * @param channelId
	 * @param categoryId
	 * @return
	 */
	@RequestMapping(value = "/read/{id:.+}", method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<Response> read(@PathVariable(value = "id") String termId, @RequestHeader(value = "X-Channel-Id") String channelId,
			@RequestParam(value = "category", required = true) String categoryId) {
		String apiId = "ekstep.learning.channel.term.read";
		try {
			if (termManager.validateRequest(channelId, categoryId)) {
				Response response = termManager.readTerm(graphId, termId, categoryId);
				return getResponseEntity(response, apiId, null);
			} else {
				throw new ClientException("ERR_INVALID_CATEGORY_ID", "Invalid CategoryId for Term", apiId, null);
			}
		} catch (Exception e) {
			PlatformLogger.log("Read term", e.getMessage(), e);
			return getExceptionResponseEntity(e, apiId, null);
		}
	}

	/**
	 * 
	 * @param termId
	 * @param channelId
	 * @param categoryId
	 * @param requestMap
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/update/{id:.+}", method = RequestMethod.PATCH)
	@ResponseBody
	public ResponseEntity<Response> update(@PathVariable(value = "id") String termId,
			@RequestHeader(value = "X-Channel-Id") String channelId,
			@RequestParam(value = "category", required = true) String categoryId,
			@RequestBody Map<String, Object> requestMap) {
		String apiId = "ekstep.learning.channel.term.update";
		Request request = getRequest(requestMap);
		try {
			if (termManager.validateRequest(channelId, categoryId)) {
				Map<String, Object> map = (Map<String, Object>) request.get("term");
				Response response = termManager.updateTerm(categoryId, termId, map);
				return getResponseEntity(response, apiId, null);
			} else {
				throw new ClientException("ERR_INVALID_CATEGORY_ID", "Invalid CategoryId for TermUpdate", apiId, null);
			}
		} catch (Exception e) {
			PlatformLogger.log("Update term", e.getMessage(), e);
			return getExceptionResponseEntity(e, apiId, null);
		}
	}

	/**
	 * 
	 * @param categoryId
	 * @param channelId
	 * @param requestMap
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@RequestMapping(value = "/search", method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<Response> list(@RequestParam(value = "category", required = true) String categoryId,
			@RequestHeader(value = "X-Channel-Id") String channelId, @RequestBody Map<String, Object> requestMap) {
		String apiId = "ekstep.learning.channel.term.search";
		Request request = getRequest(requestMap);
		try {
			if (termManager.validateRequest(channelId, categoryId)) {
				Map<String, Object> map = (Map<String, Object>) request.get("search");
				Response response = termManager.searchTerms(categoryId, map);
				return getResponseEntity(response, apiId, null);
			} else {
				throw new ClientException("ERR_INVALID_CATEGORY_ID", "Invalid CategoryId for TermSearch", apiId, null);
			}
		} catch (Exception e) {
			PlatformLogger.log("Search terms", e.getMessage(), e);
			return getExceptionResponseEntity(e, apiId, null);
		}
	}

	/**
	 * 
	 * @param termId
	 * @param channelId
	 * @param categoryId
	 * @return
	 */
	@RequestMapping(value = "/retire/{id:.+}", method = RequestMethod.DELETE)
	@ResponseBody
	public ResponseEntity<Response> retire(@PathVariable(value = "id") String termId,
			@RequestHeader(value = "X-Channel-Id") String channelId,
			@RequestParam(value = "category", required = true) String categoryId) {
		String apiId = "ekstep.learning.channel.term.retire";
		try {
			if (termManager.validateRequest(channelId, categoryId)) {
				Response response = termManager.retireTerm(categoryId, termId);
				return getResponseEntity(response, apiId, null);
			} else {
				throw new ClientException("ERR_INVALID_CATEGORY_ID", "Invalid CategoryId for TermRetire", apiId, null);
			}
		} catch (Exception e) {
			PlatformLogger.log("retire term", e.getMessage(), e);
			return getExceptionResponseEntity(e, apiId, null);
		}
	}
}