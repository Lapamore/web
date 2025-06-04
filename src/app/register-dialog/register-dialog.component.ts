import { Component } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { UserService } from '../user/service';

@Component({
  selector: 'app-register-dialog',
  template: `
    <h2 mat-dialog-title>Регистрация</h2>
    <mat-dialog-content class="mat-typography">
      <form [formGroup]="registerForm" (ngSubmit)="onRegister()">
        <mat-form-field appearance="fill" class="full-width">
          <mat-label>Логин</mat-label>
          <input matInput formControlName="username" required>
          <mat-error *ngIf="registerForm.get('username')?.hasError('required')">
            Логин обязателен
          </mat-error>
          <mat-error *ngIf="registerForm.get('username')?.hasError('minlength')">
            Логин должен содержать минимум 3 символа
          </mat-error>
        </mat-form-field>

        <mat-form-field appearance="fill" class="full-width">
          <mat-label>Пароль</mat-label>
          <input matInput type="password" formControlName="password" required>
          <mat-error *ngIf="registerForm.get('password')?.hasError('required')">
            Пароль обязателен
          </mat-error>
          <mat-error *ngIf="registerForm.get('password')?.hasError('minlength')">
            Пароль должен содержать минимум 6 символов
          </mat-error>
        </mat-form-field>

        <mat-form-field appearance="fill" class="full-width">
          <mat-label>Подтвердите пароль</mat-label>
          <input matInput type="password" formControlName="confirmPassword" required>
          <mat-error *ngIf="registerForm.get('confirmPassword')?.hasError('required')">
            Подтверждение пароля обязательно
          </mat-error>
          <mat-error *ngIf="registerForm.hasError('mismatch') && registerForm.get('confirmPassword')?.touched">
            Пароли не совпадают
          </mat-error>
        </mat-form-field>

        <div *ngIf="errorMessage" class="error-message">
          {{ errorMessage }}
        </div>
      </form>
    </mat-dialog-content>
    <mat-dialog-actions align="end">
      <button mat-button mat-dialog-close>Отмена</button>
      <button mat-raised-button color="primary" 
              [disabled]="registerForm.invalid" 
              (click)="onRegister()">
        Зарегистрироваться
      </button>
    </mat-dialog-actions>
  `,
  styles: [`
    .full-width {
      width: 100%;
      margin-bottom: 16px;
    }
    .error-message {
      color: #f44336;
      font-size: 14px;
      margin-top: 8px;
    }
    mat-dialog-content {
      min-width: 300px;
      padding: 20px;
    }
  `]
})
export class RegisterDialogComponent {
  registerForm: FormGroup;
  errorMessage: string = '';

  constructor(
    private fb: FormBuilder,
    private userService: UserService,
    public dialogRef: MatDialogRef<RegisterDialogComponent>
  ) {
    this.registerForm = this.fb.group({
      username: ['', [Validators.required, Validators.minLength(3)]],
      password: ['', [Validators.required, Validators.minLength(6)]],
      confirmPassword: ['', Validators.required]
    }, { validators: this.passwordMatchValidator });
  }

  passwordMatchValidator(form: FormGroup) {
    const password = form.get('password');
    const confirmPassword = form.get('confirmPassword');
    
    if (password && confirmPassword && password.value !== confirmPassword.value) {
      return { mismatch: true };
    }
    return null;
  }

  onRegister(): void {
    if (this.registerForm.valid) {
      const { username, password, confirmPassword } = this.registerForm.value;
      const result = this.userService.register(username, password, confirmPassword);
      
      if (result.success) {
        this.dialogRef.close(true);
      } else {
        this.errorMessage = result.message || 'Ошибка регистрации';
      }
    }
  }
}