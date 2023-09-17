import torch
import torch.nn as nn

# 시퀀스 길이, 배치 크기, 입력 차원 및 LSTM 레이어 크기 정의
sequence_length = 10
batch_size = 32
input_size = 64
hidden_size = 128
num_layers = 1  # LSTM 레이어 수

# 입력 데이터 생성 (임의의 값)
input_data = torch.randn(sequence_length, batch_size, input_size)

# LSTM 모델 정의
class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
    
    def forward(self, x):
        # 초기 hidden 상태 및 cell 상태 초기화
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        # LSTM 모델 실행
        out, _ = self.lstm(x, (h0, c0))
        return out

# LSTM 모델 인스턴스 생성
model = LSTMModel(input_size, hidden_size, num_layers)

# 모델을 GPU로 이동 (필요한 경우)
# model = model.cuda()

# 입력 데이터를 모델에 전달하여 예측을 생성
output = model(input_data)

# 출력의 크기 확인 (시퀀스 길이, 배치 크기, hidden_size)
print(output.size())
