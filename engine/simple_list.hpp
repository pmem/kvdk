#pragma once
#ifndef SIMPLE_LIST_HPP
#define SIMPLE_LIST_HPP

#include <stdexcept>

#include "kvdk/iterator.hpp"

#include "generic_list.hpp"

namespace KVDK_NAMESPACE {
using List = GenericList<RecordType::ListRecord, RecordType::ListElem>;
using ListBuilder =
    GenericListBuilder<RecordType::ListRecord, RecordType::ListElem>;

class ListIterator : Iterator
{
public:
    ListIterator(List* l) : list{l}, rep{l->Front()} 
    {
        kvdk_assert(list != nullptr, "");
    }

    void Seek(const std::string& elem) final
    {
        while (rep != list->Tail() && elem != rep->Value())
        {
            ++rep;
        }
    }

    void Seek(IndexType pos)
    {
        
    }

  virtual void Seek(IndexType pos) = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

    bool Valid() final
    {
        // list->Head() == list->Tail()
        return (rep != );
    }

    void Next() final 
    {
        ++rep;
    }

    void Prev() final
    {
        ++rep;
    }

  virtual std::string Key() = 0;

  virtual std::string Value() = 0;
  
private:
    List::Iterator rep;
};


}  // namespace KVDK_NAMESPACE

#endif  // SIMPLE_LIST_HPP