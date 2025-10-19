# from https://huggingface.co/spaces/Acetde/captchabreaker/tree/main
import torch
import onnx
import onnxruntime as rt
from torchvision import transforms as T
from src.captcha_solver.tokenizer_base import Tokenizer
from PIL import Image

model_file = "src/captcha_solver/captcha.onnx"
img_size = (32, 128)
charset = r"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
tokenizer_base = Tokenizer(charset)


def get_transform(img_size):
    transforms = []
    transforms.extend(
        [
            T.Resize(img_size, T.InterpolationMode.BICUBIC),
            T.ToTensor(),
            T.Normalize(0.5, 0.5),
        ]
    )
    return T.Compose(transforms)


def to_numpy(tensor):
    return (
        tensor.detach().cpu().numpy() if tensor.requires_grad else tensor.cpu().numpy()
    )


def initialize_model(model_file):
    transform = get_transform(img_size)
    # Onnx model loading
    onnx_model = onnx.load(model_file)
    onnx.checker.check_model(onnx_model)
    ort_session = rt.InferenceSession(model_file)
    return transform, ort_session


def get_text(img_path):
    img_obj = Image.open(img_path)
    # Preprocess. Model expects a batch of images with shape: (B, C, H, W)
    x = transform(img_obj.convert("RGB")).unsqueeze(0)

    # compute ONNX Runtime output prediction
    ort_inputs = {ort_session.get_inputs()[0].name: to_numpy(x)}
    logits = ort_session.run(None, ort_inputs)[0]
    probs = torch.tensor(logits).softmax(-1)
    preds, probs = tokenizer_base.decode(probs)
    preds = preds[0]
    print(preds)
    return preds


transform, ort_session = initialize_model(model_file=model_file)


# if __name__ == "__main__":
#     image_path = "8000.png"
#     preds,probs = get_text(image_path)
#     print(preds[0])
