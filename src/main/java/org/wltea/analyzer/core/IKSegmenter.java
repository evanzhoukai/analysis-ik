/**
 * IK 中文分词  版本 5.0
 * IK Analyzer release 5.0
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * 源代码由林良益(linliangyi2005@gmail.com)提供
 * 版权声明 2012，乌龙茶工作室
 * provided by Linliangyi and copyright 2012 by Oolong studio
 */
package org.wltea.analyzer.core;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.dic.Dictionary;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词器主类
 *
 */
public final class IKSegmenter {
	
	//字符窜reader
	private Reader input;
	//分词器上下文
	private AnalyzeContext context;
	//分词处理器列表
	private List<ISegmenter> segmenters;
	//分词歧义裁决器
	private IKArbitrator arbitrator;
    private  Configuration configuration;
	private String defaluePath = "http://192.168.0.239:8088/XD_ES_DIC/base_es_remote_dic.dic";
	private static final Logger logger = ESPluginLoggerFactory.getLogger(IKSegmenter.class.getName());

	/**
	 * IK分词器构造函数
	 * @param input
     */
	public IKSegmenter(Reader input ,Configuration configuration){
		this.input = input;
        this.configuration = configuration;
        this.init();
	}

	
	/**
	 * 初始化
	 */
	private void init(){
		//初始化分词上下文
		this.context = new AnalyzeContext(configuration);
		//加载子分词器
		this.segmenters = this.loadSegmenters();
		//加载歧义裁决器
		this.arbitrator = new IKArbitrator();

		// 重新加载远程词典
		reloadRemote();

    }

	/**
	 * 重新加载远程词典
	 * 根据配置中的词典路径加载或卸载词典，以确保索引使用自定义配置的词典
	 * 配置路径必须：dictionary_path
	 * -------
	 */
	private void reloadRemote(){
		try {
			// 加载 当前索引指定的词典路径
			if (configuration.getDictionaryPath()!=null && !configuration.getDictionaryPath().isEmpty()) {
				List<String> remoteWords = getRemoteWords(configuration.getDictionaryPath());
				Dictionary.getSingleton().addWords(remoteWords);
				// 打印加载 词典 的日志
                logger.info("[<<[加载]当前索引加了额外自定义词典--Index assign Dict Loading >>>> ] {} finish success >>>>>>>> ", configuration.getDictionaryPath());
			}else{
				// 当前索引没有 配置额外 词典，清除，已经加载的 额外词典
				// 因为同一个集群 ES ，Dictionary 是 单例的。
				List<String> remoteWords = getRemoteWords(defaluePath);
				Dictionary.getSingleton().disableWords(remoteWords);
				// 打印 清除，已经加载的 额外词典
                // logger.info("[----[清除]当前索引没有配置额外自定义词典--Dict remove words  >>>> ] {} finish success >>>>>>>> ", configuration.getDictionaryPath());
			}
		} catch (Exception e) {
			// 记录异常 日志
            logger.error("[<<[加载或清除异常]Index assign Dict Loading >>>> ] {} finish fail >>>>>>>> ", configuration.getDictionaryPath(), e);
		}
	}


	/**
	 * 初始化词典，加载子分词器实现
	 * @return List<ISegmenter>
	 */
	private List<ISegmenter> loadSegmenters(){
		List<ISegmenter> segmenters = new ArrayList<ISegmenter>(4);
		//处理字母的子分词器
		segmenters.add(new LetterSegmenter()); 
		//处理中文数量词的子分词器
		segmenters.add(new CN_QuantifierSegmenter());
		//处理中文词的子分词器
		segmenters.add(new CJKSegmenter());
		return segmenters;
	}
	
	/**
	 * 分词，获取下一个词元
	 * @return Lexeme 词元对象
	 * @throws IOException
	 */
	public synchronized Lexeme next()throws IOException{
		Lexeme l = null;
		reloadRemote();
		// 记录分词日志。。。。
		while((l = context.getNextLexeme()) == null ){
			/*
			 * 从reader中读取数据，填充buffer
			 * 如果reader是分次读入buffer的，那么buffer要  进行移位处理
			 * 移位处理上次读入的但未处理的数据
			 */
			int available = context.fillBuffer(this.input);
			if(available <= 0){
				//reader已经读完
				context.reset();
				return null;
				
			}else{
				//初始化指针
				context.initCursor();
				do{
        			//遍历子分词器
        			for(ISegmenter segmenter : segmenters){
        				segmenter.analyze(context);
        			}
        			//字符缓冲区接近读完，需要读入新的字符
        			if(context.needRefillBuffer()){
        				break;
        			}
   				//向前移动指针
				}while(context.moveCursor());
				//重置子分词器，为下轮循环进行初始化
				for(ISegmenter segmenter : segmenters){
					segmenter.reset();
				}
			}
			//对分词进行歧义处理
			this.arbitrator.process(context, configuration.isUseSmart());
			//将分词结果输出到结果集，并处理未切分的单个CJK字符
			context.outputToResult();
			//记录本次分词的缓冲区位移
			context.markBufferOffset();			
		}
		return l;
	}

	/**
     * 重置分词器到初始状态
     * @param input
     */
	public synchronized void reset(Reader input) {
		this.input = input;
		context.reset();
		for(ISegmenter segmenter : segmenters){
			segmenter.reset();
		}
	}

	private static List<String> getRemoteWords(String location) {
		SpecialPermission.check();
		return AccessController.doPrivileged((PrivilegedAction<List<String>>) () -> {
			return getRemoteWordsUnprivileged(location);
		});
	}

	/**
	 * 从远程服务器上下载自定义词条
	 */
	private static List<String> getRemoteWordsUnprivileged(String location) {

		List<String> buffer = new ArrayList<String>();
		RequestConfig rc = RequestConfig.custom().setConnectionRequestTimeout(10 * 1000).setConnectTimeout(10 * 1000)
				.setSocketTimeout(60 * 1000).build();
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response;
		BufferedReader in;
		HttpGet get = new HttpGet(location);
		get.setConfig(rc);
		try {
			response = httpclient.execute(get);
			if (response.getStatusLine().getStatusCode() == 200) {

				String charset = "UTF-8";
				// 获取编码，默认为utf-8
				HttpEntity entity = response.getEntity();
				if(entity!=null){
					Header contentType = entity.getContentType();
					if(contentType!=null&&contentType.getValue()!=null){
						String typeValue = contentType.getValue();
						if(typeValue!=null&&typeValue.contains("charset=")){
							charset = typeValue.substring(typeValue.lastIndexOf("=") + 1);
						}
					}

					if (entity.getContentLength() > 0 || entity.isChunked()) {
						in = new BufferedReader(new InputStreamReader(entity.getContent(), charset));
						String line;
						while ((line = in.readLine()) != null) {
							buffer.add(line);
						}
						in.close();
						response.close();
						return buffer;
					}
				}
			}
			response.close();
		} catch (IllegalStateException | IOException e) {
		}
		return buffer;
	}

}
