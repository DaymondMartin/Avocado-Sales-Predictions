#!/usr/bin/env python
# coding: utf-8

# ## Avocado AI Semantic_best practice analyzer_9779
# 
# New notebook

# ## Best Practice Analyzer
# 
# When you run this notebook, the [Best Practice Analyzer](https://learn.microsoft.com/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#sempy-fabric-run-model-bpa) (BPA) will offer tips to improve the design and performance of your semantic model. 
# 
# By default, the BPA checks a set of 60+ rules against your semantic model and summarizes the results. These rules come from experts within Microsoft and the Fabric Community.  
# 
# You’ll get suggestions for improvement in five categories: Performance, DAX Expressions, Error Prevention, Maintenance, and Formatting. 
# 
# ### Powering this feature: Semantic Link
# This notebook leverages [Semantic Link](https://learn.microsoft.com/fabric/data-science/semantic-link-overview), a python library which lets you optimize Fabric items for performance, memory and cost. The "[run_model_bpa](https://learn.microsoft.com/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#sempy-fabric-run-model-bpa)" function used in this notebook is just one example of the useful [functions]((https://learn.microsoft.com/python/api/semantic-link-sempy/sempy.fabric)) which Semantic Link offers.
# 
# You can find more [functions](https://github.com/microsoft/semantic-link-labs#featured-scenarios) and [helper notebooks](https://github.com/microsoft/semantic-link-labs/tree/main/notebooks) in [Semantic Link Labs](https://github.com/microsoft/semantic-link-labs), a Python library that extends Semantic Link's capabilities to automate technical tasks.
# 
# ### Low-code solutions for data tasks
# You don't have to be a Python expert to use Semantic Link or Semantic Link Labs. Many functions can be used simply by entering your parameters and running the notebook.
# 

# #### Import the Semantic Link library

# In[1]:


# Ensure semantic-link>=0.9.2 is installed
from packaging.version import Version
from importlib.metadata import version

sempy_version = version('semantic-link-sempy')

if Version(sempy_version) < Version("0.9.2"):
    get_ipython().run_line_magic('pip', 'install semantic-link==0.9.2')


# In[3]:


import sempy.fabric as fabric

dataset = "Avocado AI Semantic" # Enter the name or ID of the semantic model
workspace = "The Microsoft Data + AI Kenya Hack" # Enter the workspace name or ID in which the semantic model exists


# #### Run the Best Practice Analyzer on your semantic model

# In[4]:


fabric.run_model_bpa(dataset=dataset, workspace=workspace)


# #### Learn more about notebooks in Fabric
# Notebooks in Fabric empower you to use code and low-code solutions for a wide range of data analytics and data engineering tasks such as data transformation, pipeline automation, and machine learning modeling.
# 
# * To edit this notebook, switch the mode from **Run** only to **Edit** or **Develop**.
# * You can safely delete this notebook after running it. This won’t affect your semantic model.
# 
# 
# For more information on capabilities and features, check out [Microsoft Fabric Notebook Documentation](https://learn.microsoft.com/fabric/data-engineering/how-to-use-notebook).
# 
