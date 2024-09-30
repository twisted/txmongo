## Versions, currently supported by vendors:
- Python: 3.8+
- MongoDB: 5.0+ (actually 6.0+ soon)
    - new `OP_MSG` protocol is supported in MongoDB 3.6+
- PyMongo: probably 3.12+
    - PyMongo 3.x still supports Py2.7 and MongoDB 2.6+
    - PyMongo 3.8 doesn't have `bson.encode/decode` functions
 
## We decided to support:
- Python 3.8+
- MongoDB 4.0+
- PyMongo 3.12+

## TODO
- Check what are the other packages listed in tox.ini. Do we really need them?
  - Can the pinning versions of these packages reduce tox startup time?
- Do we need OP_COMPRESSED?
- Check if we need to block connection in case of MORE_TO_COME in response as RFC says
- Check Msg size against maxMessageSizeBytes
- Implement missing test cases from [OP_MSG spec test plan](https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.md#test-plan)
- Check using BSON() class. It's docstring says that using its method is slower than bson.encode/decode
- What would we do with old style coll.update(), coll.insert(), coll.remove() methods?
  - Maybe reimplement them using *_one/many counterparts?
  - Or just remove them because they are deprecated for many years?
- Test that new implementation of TxMongo isn't worse than the old one in terms of memory usage
  - Check circular references by disabling gc and measuring len(gc.get_objects()) ?

## Prior art
- https://github.com/twisted/txmongo/pull/262


## Plan:
- ~~Запускать тесты в Github Actions против разных версий pymongo~~
- ~~Запускать тесты в Github Actions против разных версий twisted~~
- ~~Добавить в тесты pymongo 3.12, 3.13, и пару последних веток 4.x~~
- ~~Починить остальные несовместимости с pymongo 4~~
- Реализовать поддержку OP_MSG-протокола (*)
- Выпилить все старые протоколы, кроме OP_MSG (*)
- Добавить тесты против MongoDB 5.0+ (*)
- Дропнуть совместимость с MongoDB <3.6 (*)

Пункты, отмеченные звёздочками проще всего сделать одним куском, а не последовательно.
Иначе придётся усложнять код поддержкой нескольких протоколов.
