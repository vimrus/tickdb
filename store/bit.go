//  https://github.com/mschoch/gouchstore/blob/master/bit.go
//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package store

import (
	"bytes"
	"encoding/binary"
)

const gs_TOP_BIT_MASK byte = 0x80

func encode_raw08(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	return bufbytes[len(bufbytes)-1:]
}

func encode_raw16(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	return bufbytes[len(bufbytes)-2:]
}

func encode_raw31_highestbiton(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	bufbytes[len(bufbytes)-4] = bufbytes[len(bufbytes)-4] | gs_TOP_BIT_MASK
	return bufbytes[len(bufbytes)-4:]
}

func encode_raw32(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	return bufbytes[len(bufbytes)-4:]
}

func encode_raw40(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	return bufbytes[len(bufbytes)-5:]
}

func encode_raw48(val interface{}) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, val)
	bufbytes := buf.Bytes()
	return bufbytes[len(bufbytes)-6:]
}

func decode_raw08(raw []byte) uint8 {
	var rv uint8
	buf := bytes.NewBuffer(raw)
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

func decode_raw16(raw []byte) uint16 {
	var rv uint16
	buf := bytes.NewBuffer(raw)
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

// just like raw32 but mask out the top bit
func decode_raw31(raw []byte) uint32 {
	var rv uint32
	topByte := maskOutTopBit(raw[0])
	buf := bytes.NewBuffer([]byte{topByte})
	buf.Write(raw[1:4])
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

func decode_raw32(raw []byte) uint32 {
	var rv uint32
	buf := bytes.NewBuffer(raw)
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

func decode_raw48(raw []byte) uint64 {
	var rv uint64
	buf := bytes.NewBuffer([]byte{0, 0})
	buf.Write(raw)
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

func decode_raw40(raw []byte) uint64 {
	var rv uint64
	buf := bytes.NewBuffer([]byte{0, 0, 0})
	buf.Write(raw)
	binary.Read(buf, binary.BigEndian, &rv)
	return rv
}

func valueTopBit(in byte) bool {
	if in&gs_TOP_BIT_MASK != 0 {
		return true
	}
	return false
}

func maskOutTopBit(in byte) byte {
	return in &^ gs_TOP_BIT_MASK
}

// this decodes a common structure with 1 bit, followed by 47 bits
func decode_raw_1_47_split(raw []byte) (bool, uint64) {
	var rint uint64
	rbool := valueTopBit(raw[0])
	topByte := maskOutTopBit(raw[0])
	buf := bytes.NewBuffer([]byte{0, 0, topByte})
	buf.Write(raw[1:6])
	binary.Read(buf, binary.BigEndian, &rint)
	return rbool, rint
}

func encode_raw_1_47_split(topBit bool, rest uint64) []byte {
	// encode the rest portion first
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, rest)
	bufbytes := buf.Bytes()
	// then overwrite the top bit
	if topBit {
		bufbytes[len(bufbytes)-6] = bufbytes[len(bufbytes)-6] | gs_TOP_BIT_MASK
	} else {
		bufbytes[len(bufbytes)-6] = bufbytes[len(bufbytes)-6] &^ gs_TOP_BIT_MASK
	}
	return bufbytes[len(bufbytes)-6:]
}

func decode_raw_12_28_split(data []byte) (top uint32, bottom uint32) {
	kFirstByte := (data[0] & 0xf0) >> 4
	kSecondByteTop := (data[0] & 0x0f) << 4
	kSecondByteBottom := (data[1] & 0xf0) >> 4
	kSecondByte := kSecondByteTop | kSecondByteBottom

	buf := bytes.NewBuffer([]byte{0x00, 0x00, kFirstByte, kSecondByte})
	binary.Read(buf, binary.BigEndian, &top)

	buf = bytes.NewBuffer([]byte{data[1] & 0x0f})
	buf.Write(data[2:])
	binary.Read(buf, binary.BigEndian, &bottom)
	return
}

func encode_raw_12_28_split(top uint32, bottom uint32) []byte {
	topbuf := new(bytes.Buffer)
	binary.Write(topbuf, binary.BigEndian, top)
	topbytes := topbuf.Bytes()

	newtoptop := topbytes[len(topbytes)-2] & 0x0f << 4
	newtopbottom := topbytes[len(topbytes)-1] & 0xf0 >> 4
	newtop := newtoptop | newtopbottom

	newbottomtop := topbytes[len(topbytes)-1] & 0x0f << 4

	bottombuf := new(bytes.Buffer)
	binary.Write(bottombuf, binary.BigEndian, bottom)
	bottombytes := bottombuf.Bytes()

	newbottombottom := bottombytes[len(bottombytes)-4] & 0x0f

	newbottom := newbottomtop | newbottombottom

	resultbuf := bytes.NewBuffer([]byte{newtop, newbottom})
	resultbuf.Write(bottombytes[len(bottombytes)-3:])
	return resultbuf.Bytes()
}
