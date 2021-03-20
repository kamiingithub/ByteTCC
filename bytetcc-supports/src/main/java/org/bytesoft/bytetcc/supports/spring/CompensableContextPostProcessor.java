/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytetcc.supports.spring;

import java.util.ArrayList;
import java.util.List;

import org.bytesoft.bytejta.supports.spring.ManagedConnectionFactoryPostProcessor;
import org.bytesoft.bytetcc.supports.spring.aware.CompensableContextAware;
import org.bytesoft.compensable.CompensableContext;
import org.bytesoft.transaction.adapter.ResourceAdapterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;

public class CompensableContextPostProcessor implements BeanFactoryPostProcessor {
	static final Logger logger = LoggerFactory.getLogger(CompensableContextPostProcessor.class);

	/**
	 * 启动2：把context注入到实现 CompensableContextAware接口的类中
	 *
	 * 启动3：动态代理XADataSource
	 * @see ManagedConnectionFactoryPostProcessor#postProcessAfterInitialization
	 *
	 * 启动4：开启后台线程执行任务；1)ComensableWork-系统启动恢复事务、运行期间不断尝试恢复事务 2)CleanupWork 3)SampleTransactionLogger-记录日志类工作
	 * @see ResourceAdapterImpl#start
	 *
	 * @param beanFactory
	 * @throws BeansException
	 */
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		String targetBeanId = null;
		List<BeanDefinition> beanDefList = new ArrayList<BeanDefinition>();
		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			Class<?> beanClass = null;
			try {
				beanClass = cl.loadClass(beanClassName);
			} catch (Exception ex) {
				logger.debug("Cannot load class {}, beanId= {}!", beanClassName, beanName, ex);
				continue;
			}

			if (CompensableContextAware.class.isAssignableFrom(beanClass)) {
				// 实现CompensableContextAware的bean集合
				beanDefList.add(beanDef);
			}

			if (CompensableContext.class.isAssignableFrom(beanClass)) {
				if (targetBeanId == null) {
					targetBeanId = beanName;
				} else {
					throw new FatalBeanException("Duplicated compensable-context defined.");
				}
			}

		}

		for (int i = 0; targetBeanId != null && i < beanDefList.size(); i++) {
			BeanDefinition beanDef = beanDefList.get(i);
			MutablePropertyValues mpv = beanDef.getPropertyValues();
			RuntimeBeanReference beanRef = new RuntimeBeanReference(targetBeanId);
			// 注入到beanDefinition的属性中
			mpv.addPropertyValue(CompensableContextAware.COMPENSABLE_CONTEXT_FIELD_NAME, beanRef);
		}

	}

}
