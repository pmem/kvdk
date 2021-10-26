//
// Created by zhanghuigui on 2021/10/20.
//

#pragma once

#include <vector>
#include <map>
#include <cstdio>

#include <kvdk/status.hpp>
#include "KVEngine.hpp"

// An engines factory for create the engine by the engine name
template <class EngineType_t>
class EngineRegister {
public:
	virtual EngineType_t* CreateEngine(const std::string& op_name) = 0;

protected:
	EngineRegister() {}
	virtual ~EngineRegister() {}
};

// Engine factory class template
template <class EngineType_t>
class EngineFactory {
public:
	// Single pattern of the factory
	static EngineFactory<EngineType_t>& Instance() {
		static EngineFactory<EngineType_t> instance;
		return instance;
	}

	void RegisterEngine(const std::string& op_name,
											EngineRegister<EngineType_t>* reg) {
		operation_register[op_name] = reg;
	}

	EngineType_t* GetEngine(const std::string& op_name) {
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
	const EngineFactory& operator= (const EngineFactory&) = delete;

	std::map<std::string, EngineRegister<EngineType_t>* > operation_register;
};

// An template class to create the detail Operation
template <class EngineType_t, class OperationImpl_t>
class EngineImplRegister : public EngineRegister<EngineType_t> {
public:
	explicit EngineImplRegister(const std::string& op_name) {
		EngineFactory<EngineType_t>::Instance().RegisterEngine(op_name, this);
	}

	EngineType_t* CreateEngine(const std::string& op_name) {
		return new OperationImpl_t(op_name);
	}
};

// Construct the engine's map with their engin name.
static void Initial() {
	static bool init = false;
	if (!init) {
		static EngineImplRegister<KVEngine, PMemKVDK> pmem_kvdk("kvdk");
		static EngineImplRegister<KVEngine, RocksEngine> rocks_engine("rocksdb");
		//static EngineImplRegister<KVEngine, LevelEngine> level_engine("leveldb");
		init = true;
	}
}

class CreateEngineByName {
public:
	CreateEngineByName()= default;
	static KVEngine* Create(const std::string& name) {
		EngineFactory<KVEngine>& engine = EngineFactory<KVEngine>::Instance();
		return engine.GetEngine(name);
	}
};

