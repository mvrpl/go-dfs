package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"

	"mvrpl.dev/dfsapi/fileSystemServer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

type dfsGrpcNN struct {
	fileSystemServer.PrincipalNoServer
}

type dfsGrpcDN struct {
	fileSystemServer.DadosNoServer
}

func (m *dfsGrpcNN) AtribuirBlocos(ctx context.Context, request *fileSystemServer.ArquivoMetadata) (*fileSystemServer.MapeamentoLocalizacaoBloco, error) {
	return nil, nil
}

func removerServidorInativo(indc int, resv bool) error {
	if _, err := os.Stat("/tmp/datanodes.json"); err != nil {
		return nil
	}

	servidoresDados, err1 := os.ReadFile("/tmp/datanodes.json")
	var servidoresDadosObj = &fileSystemServer.ServidoresDados{}
	err2 := protojson.Unmarshal(servidoresDados, servidoresDadosObj)

	if resv {
		servidoresDadosObj.ServidoresDados = nil
	} else {
		servidoresDadosObj.ServidoresDados = append(servidoresDadosObj.ServidoresDados[:indc], servidoresDadosObj.ServidoresDados[indc+1:]...)
	}

	var err error

	err = errors.Join(err1, err2)

	if err == nil {
		arquivoNosDados, err3 := protojson.Marshal(servidoresDadosObj)
		errEscritaMeta := os.WriteFile("/tmp/datanodes.json", []byte(arquivoNosDados), 0755)
		err = errors.Join(err, err3, errEscritaMeta)
	}

	return err
}

func (m *dfsGrpcNN) AdcNoDados(ctx context.Context, request *fileSystemServer.DadosNoInfo) (*fileSystemServer.Status, error) {
	var statusAdcNoDados = &fileSystemServer.Status{Success: true}

	var servidoresDadosObj = &fileSystemServer.ServidoresDados{}

	if _, err := os.Stat("/tmp/datanodes.json"); err == nil {
		servidoresDados, err := os.ReadFile("/tmp/datanodes.json")
		if err != nil {
			panic(err)
		}
		err2 := protojson.Unmarshal(servidoresDados, servidoresDadosObj)
		if err2 != nil {
			panic(err2)
		}
	}

	servidoresDadosObj.ServidoresDados = append(servidoresDadosObj.ServidoresDados, request)

	infraNoDados, _ := protojson.Marshal(servidoresDadosObj)
	errEscritaMeta := os.WriteFile("/tmp/datanodes.json", []byte(infraNoDados), 0755)
	if errEscritaMeta != nil {
		statusAdcNoDados.Success = false
	}

	return statusAdcNoDados, nil
}

func (m *dfsGrpcNN) AdcNovoArquivo(ctx context.Context, request *fileSystemServer.Bloco) (*fileSystemServer.Status, error) {
	servidoresDados, errLerServidores := os.ReadFile("/tmp/datanodes.json")
	if errLerServidores != nil {
		panic(errLerServidores)
	}
	servidoresDadosObj := &fileSystemServer.ServidoresDados{}
	errJson := protojson.Unmarshal(servidoresDados, servidoresDadosObj)
	if errJson != nil {
		panic(errJson)
	}

	var statusEscritaNo = &fileSystemServer.Status{Success: false}

	if len(servidoresDadosObj.ServidoresDados) == 0 {
		return statusEscritaNo, nil
	}

	servidorDadosRand := rand.Intn(len(servidoresDadosObj.ServidoresDados))

	servidorDados := servidoresDadosObj.ServidoresDados[servidorDadosRand]
	status, err := m.EstaAtivo(context.Background(), &fileSystemServer.RelatorioBloco{DataNodeInfo: &fileSystemServer.DadosNoInfo{Ip: servidorDados.Ip, Port: servidorDados.Port}})
	if err != nil {
		panic(err)
	}

	if status.Success {
		servidorDadosAddr := fmt.Sprintf("%s:%d", servidorDados.Ip, servidorDados.Port)
		conn, _ := grpc.Dial(servidorDadosAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := fileSystemServer.NewDadosNoClient(conn)

		var chamadaOpts []grpc.CallOption
		statusEscritaNoDados, err := client.EscreverBloco(context.Background(), request, chamadaOpts...)
		if err != nil {
			panic(err)
		}
		statusEscritaNo = statusEscritaNoDados
	} else {
		errRemServ := removerServidorInativo(servidorDadosRand, false)
		if errRemServ != nil {
			panic(errRemServ)
		}
		m.AdcNovoArquivo(context.Background(), request)
	}

	return statusEscritaNo, nil
}

func (m *dfsGrpcNN) EstaAtivo(ctx context.Context, request *fileSystemServer.RelatorioBloco) (*fileSystemServer.Status, error) {
	servidoresDados, errLerServidores := os.ReadFile("/tmp/datanodes.json")
	if errLerServidores != nil {
		panic(errLerServidores)
	}
	servidoresDadosObj := &fileSystemServer.ServidoresDados{}
	errJson := protojson.Unmarshal(servidoresDados, servidoresDadosObj)
	if errJson != nil {
		panic(errJson)
	}

	var status bool = false

	for _, servidor := range servidoresDadosObj.ServidoresDados {
		if servidor.Ip == request.DataNodeInfo.Ip && servidor.Port == request.DataNodeInfo.Port {
			servidorDadosAddr := fmt.Sprintf("%s:%d", servidor.Ip, servidor.Port)
			_, errConn := grpc.Dial(servidorDadosAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

			if errConn == nil {
				status = true
			}
		}
	}
	return &fileSystemServer.Status{Success: status}, nil
}

func (m *dfsGrpcNN) ListaArquivos(ctx context.Context, request *fileSystemServer.ListaArquivosParam) (*fileSystemServer.ListaArquivos, error) {
	servidoresDados, errLerServidores := os.ReadFile("/tmp/datanodes.json")
	servidoresDadosObj := &fileSystemServer.ServidoresDados{}
	errJson := protojson.Unmarshal(servidoresDados, servidoresDadosObj)

	var listaArquivosServidores fileSystemServer.ListaArquivos

	for _, servidor := range servidoresDadosObj.ServidoresDados {
		servidorDadosAddr := fmt.Sprintf("%s:%d", servidor.Ip, servidor.Port)
		conn, _ := grpc.Dial(servidorDadosAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := fileSystemServer.NewDadosNoClient(conn)

		var chamadaOpts []grpc.CallOption
		listaArquivosServidor, err := client.ListaDataArquivos(context.Background(), &fileSystemServer.ListaArquivosParam{}, chamadaOpts...)
		if err != nil {
			panic(err)
		}

		listaArquivosServidores.Files = append(listaArquivosServidores.Files, listaArquivosServidor.Files...)
	}

	err := errors.Join(errLerServidores, errJson)

	return &listaArquivosServidores, err
}

func (m *dfsGrpcNN) ObterLocalizacoesBloco(ctx context.Context, request *fileSystemServer.ArquivoMetadata) (*fileSystemServer.MapeamentoLocalizacaoBloco, error) {
	servidoresDados, errLerServidores := os.ReadFile("/tmp/datanodes.json")
	servidoresDadosObj := &fileSystemServer.ServidoresDados{}
	errJson := protojson.Unmarshal(servidoresDados, servidoresDadosObj)

	mapeamentoLocBlocos := &fileSystemServer.MapeamentoLocalizacaoBloco{}
	var listaBlocosArquivo = mapeamentoLocBlocos.Mapping

	for _, servidor := range servidoresDadosObj.ServidoresDados {
		servidorDadosAddr := fmt.Sprintf("%s:%d", servidor.Ip, servidor.Port)
		conn, _ := grpc.Dial(servidorDadosAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := fileSystemServer.NewDadosNoClient(conn)

		var chamadaOpts []grpc.CallOption
		listaArquivosServidor, err := client.ListaDataArquivos(context.Background(), &fileSystemServer.ListaArquivosParam{}, chamadaOpts...)
		if err != nil {
			panic(err)
		}

		for _, arquivo := range listaArquivosServidor.Files {
			if arquivo.BlockIndex == request.BlockIndex && arquivo.Name == request.Name {
				obterBlocoInfo, err := client.LerBloco(context.Background(), &fileSystemServer.BlocoMetadata{Index: request.BlockIndex, FileName: request.Name}, chamadaOpts...)
				if err != nil {
					panic(err)
				}
				locBloco := &fileSystemServer.LocalizacaoBloco{DataNodeInfo: servidor, BlockInfo: obterBlocoInfo.BlockInfo}
				listaBlocosArquivo = append(listaBlocosArquivo, locBloco)
			}
		}
	}

	err := errors.Join(errLerServidores, errJson)

	mapeamentoLocBlocos.Mapping = listaBlocosArquivo
	mapeamentoLocBlocos.FileInfo = request

	return mapeamentoLocBlocos, err
}

func (m *dfsGrpcDN) LerBloco(ctx context.Context, request *fileSystemServer.BlocoMetadata) (*fileSystemServer.Bloco, error) {
	arquivoMetadados := fmt.Sprintf("/tmp/metadata/%d.json", request.Index)
	metadados, errLerMeta := os.ReadFile(arquivoMetadados)
	metadadosBloco := &fileSystemServer.BlocoMetadata{}
	errJson := protojson.Unmarshal(metadados, metadadosBloco)

	arquivo := fmt.Sprintf("/tmp/%s", request.FileName)
	conteudoArq, errLerArq := os.ReadFile(arquivo)

	bloco := &fileSystemServer.Bloco{BlockInfo: metadadosBloco, Content: conteudoArq}

	err := errors.Join(errLerMeta, errJson, errLerArq)

	return bloco, err
}

func (m *dfsGrpcDN) EscreverBloco(ctx context.Context, request *fileSystemServer.Bloco) (*fileSystemServer.Status, error) {
	os.MkdirAll("/tmp/metadata/files", 0755)

	arquivo := fmt.Sprintf("/tmp/%s", request.BlockInfo.FileName)
	errEscritaData := os.WriteFile(arquivo, request.Content, 0755)

	arquivoMetadados := fmt.Sprintf("/tmp/metadata/%d.json", request.BlockInfo.Index)
	jsonMetadados, _ := protojson.Marshal(request.BlockInfo)
	errEscritaMeta := os.WriteFile(arquivoMetadados, []byte(jsonMetadados), 0755)

	arquivoMetadadosArq := fmt.Sprintf("/tmp/metadata/files/%s-%d.json", request.BlockInfo.FileName, request.BlockInfo.Index)
	jsonArqData := &fileSystemServer.ArquivoMetadata{Name: request.BlockInfo.FileName, Size: int64(len(request.Content)), BlockIndex: request.BlockInfo.Index}
	jsonMetadadosArq, _ := protojson.Marshal(jsonArqData)
	errEscritaMetaArq := os.WriteFile(arquivoMetadadosArq, []byte(jsonMetadadosArq), 0755)

	err := errors.Join(errEscritaData, errEscritaMeta, errEscritaMetaArq)

	var status bool = true

	if err != nil {
		status = false
	}

	return &fileSystemServer.Status{Success: status}, err
}

func (m *dfsGrpcDN) ListaDataArquivos(ctx context.Context, request *fileSystemServer.ListaArquivosParam) (*fileSystemServer.ListaArquivos, error) {
	os.MkdirAll("/tmp/metadata/files", 0755)

	files, errGlob := ioutil.ReadDir("/tmp/metadata/files")

	var lsDataArquivos = fileSystemServer.ListaArquivos{}.Files

	for _, arquivo := range files {
		arquivoMetadadosArq := fmt.Sprintf("/tmp/metadata/files/%s", arquivo.Name())
		metadados, errLerMeta := os.ReadFile(arquivoMetadadosArq)
		if errLerMeta != nil {
			panic(errLerMeta)
		}
		metadadosArq := &fileSystemServer.ArquivoMetadata{}
		errJson := protojson.Unmarshal(metadados, metadadosArq)
		if errJson != nil {
			panic(errJson)
		}

		lsDataArquivos = append(lsDataArquivos, metadadosArq)
	}

	err := errors.Join(errGlob)

	lsFinalArqs := &fileSystemServer.ListaArquivos{Files: lsDataArquivos}

	return lsFinalArqs, err
}

func main() {
	var porta int
	var tipoServ string
	var endNoPrincipal string
	var ipNoDados string

	flag.IntVar(&porta, "p", 7000, "Provide a port number")
	flag.StringVar(&tipoServ, "t", "namenode", "Provide a type of server [datanode or namenode]")
	flag.StringVar(&endNoPrincipal, "n", "localhost", "Provide a addr IP of namenode")
	flag.StringVar(&ipNoDados, "i", "localhost", "Provide a addr IP of namenode")

	flag.Parse()

	var lis net.Listener
	var err error

	s := grpc.NewServer()
	servidorPrincipalNo := &dfsGrpcNN{}
	servidorDadosNo := &dfsGrpcDN{}

	if tipoServ == "namenode" {
		lis, err = net.Listen("tcp", ":9000")
		fileSystemServer.RegisterPrincipalNoServer(s, servidorPrincipalNo)
		errRemServ := removerServidorInativo(0, true)
		if errRemServ != nil {
			panic(errRemServ)
		}
	} else if tipoServ == "datanode" {
		portaNoDados := fmt.Sprintf("%s:%d", ipNoDados, porta)
		lis, err = net.Listen("tcp", portaNoDados)
		fileSystemServer.RegisterDadosNoServer(s, servidorDadosNo)

		endTcpNoPrincipal := fmt.Sprintf("%s:9000", endNoPrincipal)
		conn, errTcp := grpc.Dial(endTcpNoPrincipal, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if errTcp != nil {
			panic(errTcp)
		}
		client := fileSystemServer.NewPrincipalNoClient(conn)
		_, errAdcNoDados := client.AdcNoDados(context.Background(), &fileSystemServer.DadosNoInfo{Ip: ipNoDados, Port: int32(porta)})
		if errAdcNoDados != nil {
			panic(errAdcNoDados)
		}
	}

	if err != nil {
		log.Fatalf("falha tcp: %v", err)
	}

	log.Printf("servidor iniciado em %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha no servidor: %v", err)
	}
}
