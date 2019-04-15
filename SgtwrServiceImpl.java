package com.dcits.govsbu.sd.wz.hbssb.service.impl;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.dcits.govsbu.sd.taxbankplatform.base.baseservice.impl.AbstractService;
import com.dcits.govsbu.sd.wz.gg.model.GgJbxx;
import com.dcits.govsbu.sd.wz.hbssb.mapper.CsGyGlbZspmMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.CsGyGlbZszmMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.CsSbHbswrdlztzzpzMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.HjsdjDqswrxxcjbMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.HjsdjGtfwWrwcsqkMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.HjsdjJcxxcjSyxxbMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.HjsdjJcxxcjbtMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.SbHbsDqSwrlbMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.SbHbsGtwrlbMapper;
import com.dcits.govsbu.sd.wz.hbssb.mapper.SbHbsUploadMapper;
import com.dcits.govsbu.sd.wz.hbssb.model.CsGyGlbZspm;
import com.dcits.govsbu.sd.wz.hbssb.model.CsGyGlbZszm;
import com.dcits.govsbu.sd.wz.hbssb.model.CsSbHbswrdlztzzpz;
import com.dcits.govsbu.sd.wz.hbssb.model.HjsdjDqswrxxcjb;
import com.dcits.govsbu.sd.wz.hbssb.model.HjsdjGtfwWrwcsqk;
import com.dcits.govsbu.sd.wz.hbssb.model.HjsdjJcxxcjSyxxb;
import com.dcits.govsbu.sd.wz.hbssb.model.HjsdjJcxxcjbt;
import com.dcits.govsbu.sd.wz.hbssb.model.SbHbsDqSwrlb;
import com.dcits.govsbu.sd.wz.hbssb.model.SbHbsGtwrlb;
import com.dcits.govsbu.sd.wz.hbssb.model.SbHbsUpload;
import com.dcits.govsbu.sd.wz.hbssb.service.SgtwrService;
import com.dcits.govsbu.sd.wz.hbssb.util.ImportExecl;
import com.dcits.govsbu.sd.wz.hbssb.util.Toolkit;
import com.dcits.govsbu.sd.zjpt.base.exception.BusinessException;

/**
 * 水固体污染
 * 
 * @author ltw
 * 
 */
@Service("sgtwrService")
public class SgtwrServiceImpl extends AbstractService<GgJbxx, Long> implements
		SgtwrService {
	
	private Logger log = LoggerFactory.getLogger(DqwrServiceImpl.class);
	
	private final String wrwlbDm_swr = "W";
	private final String wrwlbDm_gtwr = "S";
	private final String zsxmDm = "10121";
	private final String jsbz1 = "0";//计税标志
	
	@Autowired
	SbHbsUploadMapper sbhsbuploadMapper;
	
	@Autowired
	SbHbsGtwrlbMapper gtwrlbMapper;
	
	@Autowired
	SbHbsDqSwrlbMapper dqSwrlbMapper;
	
	@Autowired
    HjsdjJcxxcjbtMapper hjsdjJcxxcjbtMapper;
    @Autowired
    HjsdjJcxxcjSyxxbMapper hjsdjJcxxcjSyxxbMapper;
    @Autowired
    HjsdjDqswrxxcjbMapper hjsdjDqswrxxcjbMapper;
    @Autowired
    HjsdjGtfwWrwcsqkMapper hjsdjGtfwWrwcsqkMapper;
    @Autowired
    CsSbHbswrdlztzzpzMapper csSbHbswrdlztzzpzMapper;
    @Autowired
    CsGyGlbZspmMapper csGyGlbZspmMapper;
    @Autowired
    CsGyGlbZszmMapper csGyGlbZszmMapper;
	//定义月份对应的列
	private final Map<Integer,Integer> monthAndColumn = new HashMap<Integer,Integer>(){
		{
			put(1,4);
			put(2,5);
			put(3,6);
			put(4,7);
			put(5,8);
			put(6,9);
			put(7,10);
			put(8,11);
			put(9,12);
			put(10,13);
			put(11,14);
			put(12,15);
		}
	};
	
	/**
	 * 水固体污染文件解析
	 * @throws Exception 
	 * 
	 * @throws Exception
	 */
	@Override
	public void insertSgtwrFileJx(MultipartFile[] file, SbHbsUpload sbhbs,String djxh) throws Exception
			 {
		Calendar calendar = Calendar.getInstance();
		
		calendar.setTime(sbhbs.getSsqq());
		//获取ssqq月份
		
		int month = calendar.get(Calendar.MONTH) + 1;
		SbHbsDqSwrlb sbhscws1;// 第1个生产污水Sheet数据
		SbHbsDqSwrlb sbhscws2;// 第2个生产污水Sheet数据
		SbHbsDqSwrlb sbhscws3;// 第3个生产污水Sheet数据
		
		SbHbsDqSwrlb sbhshws1;// 第1个生活污水Sheet数据
		SbHbsDqSwrlb sbhshws2;// 第2个生活污水Sheet数据
		SbHbsDqSwrlb sbhshws3;// 第3个生活污水Sheet数据
		
		SbHbsGtwrlb sbhgtwr1;// 第1个固体污染Sheet数据
		SbHbsGtwrlb sbhgtwr2;// 第2个固体污染Sheet数据
		SbHbsGtwrlb sbhgtwr3;// 第3个固体污染Sheet数据
		
		List<SbHbsUpload> sbhbsUploadList = new ArrayList<>();//上传文件
		List<SbHbsDqSwrlb> sbhwsList = new ArrayList<>();//污水
		List<SbHbsGtwrlb> sbhgtwrList = new ArrayList<>();//固体污染
		HjsdjJcxxcjbt hjsdjJcxxcjbt = hjsdjJcxxcjbtMapper.selectByDjxh(djxh);
        if(hjsdjJcxxcjbt==null||"".equals(hjsdjJcxxcjbt)){
            throw new BusinessException("2011","当前企业还未进行环保税源登记，请先进行税源登记！");
        }
        if(!hjsdjJcxxcjbt.getZywrwlbsDm().contains(wrwlbDm_swr)){
            throw new BusinessException("2011","当前企业还未进行水污染物税源信息登记，请先进行税源登记！");
        }
        if(!hjsdjJcxxcjbt.getZywrwlbsDm().contains(wrwlbDm_gtwr)){
            throw new BusinessException("2011","当前企业还未进行固体污染物税源信息登记，请先进行税源登记！");
        }
        
        Map<String,Object> param = new HashMap<String,Object>();
        param.put("hbsjcxxuuid",hjsdjJcxxcjbt.getHbsjcxxuuid());
        param.put("zywrwlbDm",wrwlbDm_swr);
        param.put("yxqq",sbhbs.getSsqq());
        param.put("yxqz",sbhbs.getSsqz());
        //环保税基础信息采集表
        List<HjsdjJcxxcjSyxxb> syxxList = hjsdjJcxxcjSyxxbMapper.selectByJcxxuuid(param);
        if(syxxList.size()<=0){
            throw new BusinessException("2011","当前企业还未进行水污染物税源信息登记，请先进行税源登记！");
        }
        //判断是否采集环保税大气水污染信息
        List<HjsdjDqswrxxcjb> swrcjxxList =hjsdjDqswrxxcjbMapper.selectBySyuuid(syxxList,wrwlbDm_swr);
        if(swrcjxxList.size()<=0){
            throw new BusinessException("2011","当前企业还未进行水污染物基础信息采集，请先进行采集！");
        }
        //判断是否采集环保税固体污染信息
        List<HjsdjGtfwWrwcsqk> gtwrcjxxList = hjsdjGtfwWrwcsqkMapper.selectByHbsjcxxuuid(hjsdjJcxxcjbt.getHbsjcxxuuid());
        if(gtwrcjxxList.size()<=0){
        	throw new BusinessException("2011","当前企业还未进行固体废物产生情况采集，请先进行采集！");
        }
        if(gtwrcjxxList.size()>=2){
        	throw new BusinessException("2011","您填写的固体废物产生情况采集和表格不符，请只保留一条产生情况！");
        }
        HjsdjGtfwWrwcsqk gtwrcjxx = gtwrcjxxList.get(0);
        
        String gtwr_jldwDm = null;
        BigDecimal gtwr_sl1 = null;
        
        CsSbHbswrdlztzzpz recod = new CsSbHbswrdlztzzpz();
		recod.setZspmDm(gtwrcjxx.getZspmDm());
		recod.setZszmDm(gtwrcjxx.getZszmDm());
		CsSbHbswrdlztzzpz csSbHbswrdlztzzpz = csSbHbswrdlztzzpzMapper.selectDqwrWrdlzJldw(recod);
		if (csSbHbswrdlztzzpz == null) {
			throw new BusinessException("2011","在污染当量值配置表中未找到固体污染参数！请联系管理员");
		}
		gtwr_jldwDm = csSbHbswrdlztzzpz.getJldwDm();//计量单位代码
		
		//税率
		Map<String,Object> csMap = new HashMap<>();
		csMap.put("zsxmDm",zsxmDm);
		csMap.put("zspmDm",gtwrcjxx.getZspmDm());
		csMap.put("zszmDm",gtwrcjxx.getZszmDm());
		
		//zszmdm为空查询cs_gy_glb_zspm表      否查询cs_gy_glb_zszm
		if (gtwrcjxx.getZszmDm() == null || "".equals(gtwrcjxx.getZszmDm())){
			gtwr_sl1 = csGyGlbZspmMapper.selectSlByZspmDm(csMap); //税率
		} else {
			
			gtwr_sl1 = csGyGlbZszmMapper.selectSlByZspmDmAndZszmDm(csMap); //税率
		}
		
		if (gtwr_sl1 == null) {
			throw new BusinessException("2011","在征收品目配置表中未找到固体污染参数！请联系管理员");
		}
		for (MultipartFile multipartFile : file) {
			
			// 获取文件流
			InputStream stream = multipartFile.getInputStream();
			// 获取文件名
			String fileName = multipartFile.getOriginalFilename();
			System.out.println(fileName);
			
			BigDecimal scwjxh = Toolkit.getSequenceID20();
			if (scwjxh == null) {
				throw new BusinessException("2011","系统异常，序列号获取失败！");
			}
			
			//获取上传文件表信息
			SbHbsUpload hbsUpload = new SbHbsUpload();
			hbsUpload.setXh(Toolkit.getSequenceID20());
			hbsUpload.setSsqq(sbhbs.getSsqq());
			hbsUpload.setSsqz(sbhbs.getSsqz());
			hbsUpload.setName(fileName);
			hbsUpload.setNsrsbh(sbhbs.getNsrsbh());
			hbsUpload.setWrlb("2");//污染类别（1：大气，2：水固体，3：钻井泥浆）
			
			int count = sbhsbuploadMapper.queryhasExcel(hbsUpload);
			
			if (count > 0){
				throw new BusinessException("2011","您已上传过的文件，请删除之前导入的文件数据，才能重新导入！");
			}
			
			Workbook workbook = new XSSFWorkbook(stream);
			// 获取sheet页 固定4个sheet页
			Sheet sheet1 = workbook.getSheetAt(0);
			Sheet sheet2 = workbook.getSheetAt(1);
			Sheet sheet3 = workbook.getSheetAt(2);
			Sheet sheet4 = workbook.getSheetAt(3);
			//---------------解析第1个月 水、固体污染-------------------
			Map<String, Object> sgtwr1 = getSgtwr(sheet1, sheet4, month);
			sbhscws1 = (SbHbsDqSwrlb) sgtwr1.get("sbhscws");
			sbhshws1 = (SbHbsDqSwrlb) sgtwr1.get("sbhshws");
			sbhgtwr1 = (SbHbsGtwrlb) sgtwr1.get("sbhgtwr");
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhscws1)){
				throw new BusinessException("2011","["+fileName+"]中第1页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhshws1)){
				throw new BusinessException("2011","["+fileName+"]中第1页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			sbhgtwr1.setZsxmDm(zsxmDm);
			sbhgtwr1.setZspmDm(gtwrcjxx.getZspmDm());
			sbhgtwr1.setZszmDm(gtwrcjxx.getZszmDm());
			sbhgtwr1.setJldwDm(gtwr_jldwDm);
			sbhgtwr1.setSl1(gtwr_sl1);
			
			//---------------解析第2个月 水、固体污染-------------------
			Map<String, Object> sgtwr2 = getSgtwr(sheet2, sheet4, month + 1);
			sbhscws2 = (SbHbsDqSwrlb) sgtwr2.get("sbhscws");
			sbhshws2 = (SbHbsDqSwrlb) sgtwr2.get("sbhshws");
			sbhgtwr2 = (SbHbsGtwrlb) sgtwr2.get("sbhgtwr");
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhscws2)){
				throw new BusinessException("2011","["+fileName+"]中第2页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhshws2)){
				throw new BusinessException("2011","["+fileName+"]中第2页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			sbhgtwr2.setZsxmDm(zsxmDm);
			sbhgtwr2.setZspmDm(gtwrcjxx.getZspmDm());
			sbhgtwr2.setZszmDm(gtwrcjxx.getZszmDm());
			sbhgtwr2.setJldwDm(gtwr_jldwDm);
			sbhgtwr2.setSl1(gtwr_sl1);
			
			//---------------解析第3个月 水、固体污染-------------------
			Map<String, Object> sgtwr3 = getSgtwr(sheet3, sheet4, month + 2);
			sbhscws3 = (SbHbsDqSwrlb) sgtwr3.get("sbhscws");
			sbhshws3 = (SbHbsDqSwrlb) sgtwr3.get("sbhshws");
			sbhgtwr3 = (SbHbsGtwrlb) sgtwr3.get("sbhgtwr");
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhscws3)){
				throw new BusinessException("2011","["+fileName+"]中第3页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			if(!checkSwrPfkmcIdentical(swrcjxxList, sbhshws3)){
				throw new BusinessException("2011","["+fileName+"]中第3页的排放口名称与水污染基础信息采集中对应税源编号的采集污染物不一致！");
			}
			sbhgtwr3.setZsxmDm(zsxmDm);
			sbhgtwr3.setZspmDm(gtwrcjxx.getZspmDm());
			sbhgtwr3.setZszmDm(gtwrcjxx.getZszmDm());
			sbhgtwr3.setJldwDm(gtwr_jldwDm);
			sbhgtwr3.setSl1(gtwr_sl1);
			
			//判断是否存在重复的排放口
			Map<String,Object> checkMap = new HashMap<String, Object>();
			checkMap.put("nsrsbh", hbsUpload.getNsrsbh());
			checkMap.put("ssqq", hbsUpload.getSsqq());
			checkMap.put("ssqz", hbsUpload.getSsqz());
			List<String> swrPfkmcList = new ArrayList<String>();
			swrPfkmcList.add(sbhscws1.getPfkmc());
			swrPfkmcList.add(sbhscws2.getPfkmc());
			swrPfkmcList.add(sbhscws3.getPfkmc());
			swrPfkmcList.add(sbhshws1.getPfkmc());
			swrPfkmcList.add(sbhshws2.getPfkmc());
			swrPfkmcList.add(sbhshws3.getPfkmc());
			checkMap.put("swrPfkmcList", swrPfkmcList);
			int swrCount = dqSwrlbMapper.checkRepeatSwr(checkMap);
			if (swrCount > 0) {
				throw new BusinessException("2011","请检查上传的文件中的文件名或者排放口是否有重复！");
			}
			
			
			
			sbhbsUploadList.add(hbsUpload);//获取的文件信息保存到集合中
			//设置基本信息
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhscws1);
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhscws2);
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhscws3);
			
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhshws1);
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhshws2);
			setSwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhshws3);
			
			setGtwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhgtwr1);
			setGtwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhgtwr2);
			setGtwrJbxx(Toolkit.getSequenceID20(), hbsUpload.getXh(), hbsUpload.getNsrsbh(), hbsUpload.getSsqq(), hbsUpload.getSsqz(), djxh, sbhgtwr3);
			
			//获取的文件信息保存到集合中
			sbhwsList.add(sbhscws1);
			sbhwsList.add(sbhscws2);
			sbhwsList.add(sbhscws3);
			
			sbhwsList.add(sbhshws1);
			sbhwsList.add(sbhshws2);
			sbhwsList.add(sbhshws3);
			
			sbhgtwrList.add(sbhgtwr1);
			sbhgtwrList.add(sbhgtwr2);
			sbhgtwrList.add(sbhgtwr3);
			
		}
		//将集合数据插入数据库
		insertSgtwr(sbhbsUploadList, sbhwsList, sbhgtwrList);
	}
	
	/**
	 * 判断税源登记的排放口名称和文件中是否一致,如果一致将税源信息的数据写入到水污染信息表中
	 * @param swrcjxxList
	 * @param sbhswr
	 * @return
	 */
	public boolean checkSwrPfkmcIdentical(List<HjsdjDqswrxxcjb> swrcjxxList,SbHbsDqSwrlb sbhswr){
		for (HjsdjDqswrxxcjb hjsdjDqswrxxcjb : swrcjxxList) {
			if(hjsdjDqswrxxcjb.getPfkmc().trim().equals(sbhswr.getPfkmc().trim())){
				//污染单量值、计量单位代码
				CsSbHbswrdlztzzpz recod = new CsSbHbswrdlztzzpz();
				recod.setZspmDm(hjsdjDqswrxxcjb.getZspmDm());
				recod.setZszmDm(hjsdjDqswrxxcjb.getZszmDm());
				CsSbHbswrdlztzzpz csSbHbswrdlztzzpz = csSbHbswrdlztzzpzMapper.selectDqwrWrdlzJldw(recod);
				if (csSbHbswrdlztzzpz == null) {
					throw new BusinessException("2011","在污染当量值配置表中未找到水污染参数！请联系管理员");
				}
				BigDecimal wrdlz = csSbHbswrdlztzzpz.getWrdlz();//污染当量值
				String jldwDm = csSbHbswrdlztzzpz.getJldwDm();//计量单位代码
				
				//税率
				Map<String,Object> csMap = new HashMap<>();
				csMap.put("zsxmDm",zsxmDm);
				csMap.put("zspmDm",hjsdjDqswrxxcjb.getZspmDm());
				csMap.put("zszmDm",hjsdjDqswrxxcjb.getZszmDm());
				
				BigDecimal sl1 = null;
				//zszmdm为空查询cs_gy_glb_zspm表      否查询cs_gy_glb_zszm
				if (hjsdjDqswrxxcjb.getZszmDm() == null || "".equals(hjsdjDqswrxxcjb.getZszmDm())){
					sl1 = csGyGlbZspmMapper.selectSlByZspmDm(csMap); //税率
				} else {
					
					sl1 = csGyGlbZszmMapper.selectSlByZspmDmAndZszmDm(csMap); //税率
				}
				
				if (sl1 == null) {
					throw new BusinessException("2011","在征收品目配置表中未找到水污染参数！请联系管理员");
				}
				
				sbhswr.setBz1("S");
				sbhswr.setSybh1(hjsdjDqswrxxcjb.getHgbhssybh());
				sbhswr.setZsxmDm(zsxmDm);//水污染统一征收项目
				sbhswr.setZspmDm(hjsdjDqswrxxcjb.getZspmDm());
				sbhswr.setZszmDm(hjsdjDqswrxxcjb.getZszmDm());
				sbhswr.setBzndz(hjsdjDqswrxxcjb.getBzndz());
				sbhswr.setWrwpfljsffDm(hjsdjDqswrxxcjb.getWrwpfljsffDm());
				sbhswr.setWrdlz(wrdlz);
				sbhswr.setJldwDm(jldwDm);
				sbhswr.setSl1(sl1);
				sbhswr.setJcxxuuid(hjsdjDqswrxxcjb.getDqswrwjcxxuuid());
				sbhswr.setWrdls(sbhswr.getWrwpfl().divide(sbhswr.getWrdlz(),6));
				sbhswr.setYjndz(sbhswr.getScldz());
				return true;
			}
		}
		return false;
	}
	
	
	/**
	 * 解析水固体污染
	 * @param sheet1
	 * @param sheet2
	 * @return
	 */
	public Map<String, Object> getSgtwr(Sheet sheet1,Sheet sheet2,int month){
		Map<String,Object> sgtMap = new HashMap<String,Object>();
		SbHbsDqSwrlb sbhscws = new SbHbsDqSwrlb();//生产污水
		SbHbsDqSwrlb sbhshws = new SbHbsDqSwrlb();//生活污水
		SbHbsGtwrlb sbhgtwr = new SbHbsGtwrlb();//固体污染
		//		污染当量值（千克或吨）固定为：生产污水0.1，生活污水1.0
		double shWrdlz = 1.0;
		double scWrdlz = 0.1;
		int totalRowNum = sheet1.getLastRowNum();// 获取总行数
		String pfk = ImportExecl.getCellFormatValue(
				sheet1.getRow(2).getCell(1)).toString();
		String scpfk = ImportExecl.getCellFormatValue(
				sheet1.getRow(3).getCell(3)).toString();
		String shpfk = ImportExecl.getCellFormatValue(
				sheet1.getRow(3).getCell(7)).toString();
		//生产污水排放口名称
		scpfk = pfk + scpfk.substring(scpfk.indexOf("：") + 1);
		//生活污水排放口名称
		shpfk = pfk + shpfk.substring(shpfk.indexOf("：") + 1);
		
		//---------生产污水----------
		// 获取倒数第二排的合计列值
		double scws_nd = Double.valueOf(ImportExecl.getCellFormatValue(
				sheet1.getRow(totalRowNum - 1).getCell(3)).toString());// 生产污水浓度
		double scws_pfl = Double.valueOf(ImportExecl.getCellFormatValue(
				sheet1.getRow(totalRowNum - 1).getCell(4)).toString());// 生产污水排放量
		sbhscws.setPfkmc(scpfk);
		sbhscws.setScldz(new BigDecimal(scws_nd).setScale(6,BigDecimal.ROUND_DOWN));
		sbhscws.setPfl(new BigDecimal(scws_pfl).setScale(6,BigDecimal.ROUND_DOWN));
		sbhscws.setWrdlz(new BigDecimal(scWrdlz).setScale(6,BigDecimal.ROUND_DOWN));
		//sbhscws.setWrwpfljsffDm("1");
		//污染物排放量算法
		double scwrwpfl = scws_pfl*scws_nd/1000;
		sbhscws.setWrwpfl(new BigDecimal(scwrwpfl).setScale(6,BigDecimal.ROUND_DOWN));
		//污染当量值算法
		//sbhscws.setWrdls(new BigDecimal(scwrwpfl/scWrdlz).setScale(6,BigDecimal.ROUND_DOWN));
		sbhscws.setYf(String.valueOf(month));
		//---------生产污水----------
		
		//---------生活污水----------
		int col = monthAndColumn.get(month);//根据月份获取列
		
		double shws_pfl = Double.valueOf(ImportExecl.getCellFormatValue(
				sheet1.getRow(totalRowNum - 1).getCell(7)).toString());// 生活污水排放量
		//生活污水排放量取第4页的数据 按申报的月份
		double shws_nd1 = Double.valueOf(ImportExecl.getCellFormatValue(
				sheet2.getRow(3).getCell(col)).toString());// 生活污水浓度
		sbhshws.setPfkmc(shpfk);
		sbhshws.setPfl(new BigDecimal(shws_pfl).setScale(6,BigDecimal.ROUND_DOWN));
		sbhshws.setScldz(new BigDecimal(shws_nd1).setScale(6,BigDecimal.ROUND_DOWN));
		sbhshws.setWrdlz(new BigDecimal(shWrdlz).setScale(6,BigDecimal.ROUND_DOWN));
		//sbhshws.setWrwpfljsffDm("2");
		sbhshws.setYf(String.valueOf(month));
		//污染物排放量算法
		double shwrwpfl = shws_pfl*shws_nd1/1000;
		sbhshws.setWrwpfl(new BigDecimal(shwrwpfl).setScale(6,BigDecimal.ROUND_DOWN));
		//污染当量值算法
		//sbhshws.setWrdls(new BigDecimal(shwrwpfl/shWrdlz).setScale(6,BigDecimal.ROUND_DOWN));
		//---------生活污水----------
		
		//---------固体废物----------
		double gtwrCsl = getGtwr(sheet1);
		sbhgtwr.setDqcsl(new BigDecimal(gtwrCsl).setScale(6,BigDecimal.ROUND_DOWN));
		sbhgtwr.setYsgtfwpfl(new BigDecimal(gtwrCsl).setScale(6,BigDecimal.ROUND_DOWN));
		sbhgtwr.setYf(String.valueOf(month));
		//---------固体废物----------
		sgtMap.put("sbhscws", sbhscws);
		sgtMap.put("sbhshws", sbhshws);
		sgtMap.put("sbhgtwr", sbhgtwr);
		return sgtMap;
	}
	
	/**
	 * 固体污染
	 * @param sheet
	 * @return
	 */
	public double getGtwr(Sheet sheet){
		double gtwrCsl = 0;
		for (int i = 6; i < sheet.getLastRowNum(); i++) {
			//判断是否是数值  是数值则加总
			Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
			String colStr = ImportExecl.getCellFormatValue(sheet.getRow(i).getCell(8)).toString();
			if(pattern.matcher(colStr).matches()){
				gtwrCsl = gtwrCsl + Double.valueOf(colStr);
			}
		}
		return gtwrCsl;
	}

	
	/**
	 * 查询水固体申报资料
	 * @param sbUpload  文件bean
	 * @param pagesize  一页的条数
	 * @param currentPage 当前页数
	 * @return
	 */
	@Override
	public Map<String, Object> querySgtwrList(SbHbsUpload sbUpload,String pagesize,String currentPage) {
		Map<String, Object> map = new HashMap<String, Object>();//返回map
		try{
			int listcount = sbhsbuploadMapper.countExcel(sbUpload);
			int index = (Integer.parseInt(currentPage)-1)*Integer.parseInt(pagesize);	
			Map<String, Object> param =  new HashMap<String, Object>();//查询map
			param.put("sbUpload", sbUpload);
			param.put("index", index);
			param.put("pagesize", Integer.parseInt(pagesize));
			List<SbHbsUpload> list = sbhsbuploadMapper.queryExcelList(param);
			map.put("listcount", listcount);
			map.put("list", list);
		}catch(Exception e){
			log.error(e.getMessage());
		}
		return map;
	}
	
	private void setSwrJbxx(BigDecimal xh,BigDecimal filexh,String nsrsbh,Date ssqq,Date ssqz,String djxh,SbHbsDqSwrlb sbhsb){
		sbhsb.setXh(xh);
		sbhsb.setFilexh(filexh);
		sbhsb.setNsrsbh(nsrsbh);
		sbhsb.setSkssqq(ssqq);
		sbhsb.setSkssqz(ssqz);
		sbhsb.setDjxh(new BigDecimal(djxh));
	}
	
	private void setGtwrJbxx(BigDecimal xh,BigDecimal filexh,String nsrsbh,Date ssqq,Date ssqz,String djxh,SbHbsGtwrlb sbhsb){
		sbhsb.setXh(xh);
		sbhsb.setFilexh(filexh);
		sbhsb.setNsrsbh(nsrsbh);
		sbhsb.setSkssqq(ssqq);
		sbhsb.setSkssqz(ssqz);
		sbhsb.setDjxh(new BigDecimal(djxh));
	}



	/**
	 * 插入水固体污染表
	 */
	@Override
	public void insertSgtwr(List<SbHbsUpload> upload, List<SbHbsDqSwrlb> swr,
			List<SbHbsGtwrlb> gtwr) throws Exception{
		try {
			for (SbHbsUpload sbHbsUpload : upload) {
				sbhsbuploadMapper.insertExcel(sbHbsUpload);
			}
			gtwrlbMapper.insertGtwrList(gtwr);
			dqSwrlbMapper.insertSwrList(swr);
		} catch (Exception e) {
			throw e;
		}
		
	}

	/**
	 * 删除上传文件数据（yxbz=‘N’）
	 */
	@Override
	public void deleteSgtwrFile(BigDecimal filexh) throws Exception{
		try {
			sbhsbuploadMapper.deleteExcel(filexh);
			gtwrlbMapper.updateGtwrYxbz(filexh);
			dqSwrlbMapper.updateSwrYxbz(filexh);
		} catch (Exception e) {
			throw e;
		}
	}
}
