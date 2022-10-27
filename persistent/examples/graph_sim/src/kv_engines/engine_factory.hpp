/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdio>
#include <map>
#include <vector>

// An engines factory for create the engine by the engine name
template <class EngineType>
class EngineRegister {
 public:
  virtual EngineType* CreateEngine(const std::string& op_name) = 0;

 protected:
  EngineRegister() {}
  virtual ~EngineRegister() {}
};

// Engine factory class template
template <class EngineType>
class EngineFactory {
 public:
  // Single pattern of the factory
  static EngineFactory<EngineType>& Instance() {
    static EngineFactory<EngineType> instance;
    return instance;
  }

  void RegisterEngine(const std::string& op_name,
                      EngineRegister<EngineType>* reg) {
    operation_register[op_name] = reg;
  }

  EngineType* GetEngine(const std::string& op_name) {
    if (operation_register.find(op_name) != operation_register.end()) {
      return operation_register[op_name]->CreateEngine(op_name);
    }
    return nullptr;
  }

 private:
  // We don't allow to constructor, copy constructor and align constructor
  EngineFactory() = default;
  ~EngineFactory() = default;

  EngineFactory(const EngineFactory&) = delete;
  const EngineFactory& operator=(const EngineFactory&) = delete;

  std::map<std::string, EngineRegister<EngineType>*> operation_register;
};

// An template class to create the detail Operation
template <class EngineType, class EngineImpl>
class EngineImplRegister : public EngineRegister<EngineType> {
 public:
  explicit EngineImplRegister(const std::string& op_name) {
    EngineFactory<EngineType>::Instance().RegisterEngine(op_name, this);
  }

  EngineType* CreateEngine(const std::string& op_name) {
    return new EngineImpl(op_name);
  }
};
