package mysql

import (
	"bufio"
	"fmt"
)

type PacketHeader struct {
	Len uint64
	Seq uint8
}

func ReadPacketHeader(rd *bufio.Reader) (*PacketHeader, error) {
	var err error
	byteArr := make([]byte, 4)

	err = ReadPacket(rd, byteArr)

	if err != nil {
		return nil, err
	}

	return &PacketHeader{
		Len: UnpackNumber(byteArr, 3),
		Seq: byteArr[3],
	}, nil
}

func ReadPacket(rd *bufio.Reader, byteArr []byte) error {
	var numOfBytes int
	var err error

	for i := 0; i < len(byteArr); i++ {
		numOfBytes, err = rd.Read(byteArr[i:])

		fmt.Printf("Debug Read: %d\n", numOfBytes)

		if err != nil {
			return err
		}

		i += numOfBytes
	}

	return nil
}

func UnpackNumber(byteArr []byte, n uint8) uint64 {
	var num uint64

	for i := uint8(0); i < n; i++ {
		num |= uint64(byteArr[i]) << (i * 8)
	}

	return num
}
